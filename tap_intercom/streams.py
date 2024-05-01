"""
This module defines the stream classes and their individual sync logic.
"""


import datetime
import hashlib
import time
from typing import Iterator

import singer
from singer import Transformer, metrics, metadata, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING
from singer.transform import transform, unix_milliseconds_to_datetime

from tap_intercom.client import (IntercomClient, IntercomError)
from tap_intercom.transform import (transform_json, transform_times, find_datetimes_in_schema)

LOGGER = singer.get_logger()

MAX_PAGE_SIZE = 150


class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used to extract records from external source
    """
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    to_replicate = True
    path = None
    params = {}
    parent = None
    data_key = None
    child = None

    def __init__(self, client: IntercomClient, catalog, selected_streams):
        self.client = client
        self.catalog = catalog
        self.selected_streams = selected_streams

    def get_records(self, bookmark_datetime: datetime = None, is_parent: bool = False, stream_metadata=None) -> list:
        """
        Returns a list of records for that stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :param stream_metadata: Stream metadata dict, if required by the child get_records()
            method.
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require "
                                  "`get_records` implementation")

    def generate_record_hash(self, original_record):
        """
            Function to generate the hash of name, full_name and label to use it as a Primary Key
        """
        # There are 2 types for data_attributes in Intercom
        # -> Default: As discussed with support, there is an 'id' for custom data_attributes and that will be unique
        # -> Custom: Used 'name' and 'description' for identifying the data uniquely
        fields_to_hash = ['id', 'name', 'description']
        hash_string = ''

        for key in fields_to_hash:
            hash_string += str(original_record.get(key, ''))

        hash_string_bytes = hash_string.encode('utf-8')
        hashed_string = hashlib.sha256(hash_string_bytes).hexdigest()

        # Add Primary Key hash in the record
        original_record['_sdc_record_hash'] = hashed_string
        return original_record

    @staticmethod
    def epoch_milliseconds_to_dt_str(timestamp: float) -> str:
        # Convert epoch milliseconds to datetime object in UTC format
        new_dttm = unix_milliseconds_to_datetime(timestamp)
        return new_dttm

    @staticmethod
    def dt_to_epoch_seconds(dt_object: datetime) -> float:
        return datetime.datetime.timestamp(dt_object)

    def sync_substream(self, parent_id, stream_schema, stream_metadata, parent_replication_key, state):
        """
            Sync sub-stream data based on parent id and update the state to parent's replication value
        """
        schema_datetimes = find_datetimes_in_schema(stream_schema)
        LOGGER.info("Syncing: {}, parent_stream: {}, parent_id: {}".format(self.tap_stream_id, self.parent.tap_stream_id, parent_id))
        call_path = self.path.format(parent_id)
        response = self.client.get(call_path, params=self.params)

        data_for_transform = {self.data_key: [response]}

        transformed_records = transform_json(data_for_transform, self.tap_stream_id, self.data_key)
        LOGGER.info("Synced: {}, parent_id: {}, records: {}".format(self.tap_stream_id, parent_id, len(transformed_records)))
        with metrics.record_counter(self.tap_stream_id) as counter:
            # Iterate over conversation_parts records
            for record in transformed_records:
                transform_times(record, schema_datetimes) # Transfrom datetimes fields of record

                transformed_record = transform(record,
                                                stream_schema,
                                                integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                metadata=stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                counter.increment()

            LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, counter.value))

        # Conversations(parent) are coming in ascending order
        # so write state with updated_at of conversation after yielding conversation_parts for it.
        parent_bookmark_value = self.epoch_milliseconds_to_dt_str(parent_replication_key)
        state = singer.write_bookmark(state,
                                        self.tap_stream_id,
                                        self.replication_key,
                                        parent_bookmark_value)
        singer.write_state(state)

        return state

# pylint: disable=abstract-method,unused-argument
class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    to_write_intermediate_bookmark = False
    last_processed = None
    last_sync_started_at = None
    skipped_parent_ids = []

    def set_last_processed(self, state):
        self.last_processed = None

    def get_last_sync_started_at(self, state):
        self.last_sync_started_at = None

    def set_last_sync_started_at(self, state):
        pass

    def skip_records(self, record):
        return False

    def write_bookmark(self, state, bookmark_value):
        return singer.write_bookmark(state,
                                     self.tap_stream_id,
                                     self.replication_key,
                                     bookmark_value)

    def write_intermediate_bookmark(self, state, last_processed, bookmark_value):
        if self.to_write_intermediate_bookmark:
            # Write bookmark and state after every page of records
            state = singer.write_bookmark(state,
                                          self.tap_stream_id,
                                          self.replication_key,
                                          singer.utils.strftime(bookmark_value))
            singer.write_state(state)

    # Disabled `unused-argument` as it causing pylint error.
    # Method which call this `sync` method is passing unused argument.So, removing argument would not work.
    # pylint: disable=too-many-arguments,unused-argument
    def sync(self,
             state: dict,
             stream_schema: dict,
             stream_metadata: dict,
             config: dict,
             transformer: Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :return: State data in the form of a dictionary
        """

        # Check if the current stream has child stream or not
        has_child = self.child is not None
        # Child stream class
        child_stream = STREAMS.get(self.child)

        # Get current stream bookmark
        parent_bookmark = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        parent_bookmark_utc = singer.utils.strptime_to_utc(parent_bookmark)
        sync_start_date = parent_bookmark_utc
        self.set_last_processed(state)
        self.set_last_sync_started_at(state)

        is_parent_selected = True
        is_child_selected = False

        # If the current stream has a child stream, then get the child stream's bookmark
        # And update the sync start date to minimum of parent bookmark or child bookmark
        if has_child:
            child_bookmark = singer.get_bookmark(state, child_stream.tap_stream_id, self.replication_key, config['start_date'])
            child_bookmark_utc = singer.utils.strptime_to_utc(child_bookmark)
            child_bookmark_ts = child_bookmark_utc.timestamp() * 1000

            is_parent_selected = self.tap_stream_id in self.selected_streams
            is_child_selected = child_stream.tap_stream_id in self.selected_streams

            if is_parent_selected and is_child_selected:
                sync_start_date = min(parent_bookmark_utc, child_bookmark_utc)
            elif is_parent_selected:
                sync_start_date = parent_bookmark_utc
            elif is_child_selected:
                sync_start_date = singer.utils.strptime_to_utc(child_bookmark)

            # Create child stream object and generate schema
            child_stream_obj = child_stream(self.client, self.catalog, self.selected_streams)
            child_stream_ = self.catalog.get_stream(child_stream.tap_stream_id)
            child_schema = child_stream_.schema.to_dict()
            child_metadata = metadata.to_map(child_stream_.metadata)
            if is_child_selected:
                # Write schema for child stream as it will be synced by the parent stream
                singer.write_schema(
                    child_stream.tap_stream_id,
                    child_schema,
                    child_stream.key_properties,
                    child_stream.replication_key
                )

        LOGGER.info("Stream: {}, initial max_bookmark_value: {}".format(self.tap_stream_id, sync_start_date))
        max_datetime = sync_start_date
        # We are not using singer's record counter as the counter reset after 60 seconds
        record_counter = 0

        schema_datetimes = find_datetimes_in_schema(stream_schema)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(sync_start_date, stream_metadata=stream_metadata):
                # In case of interrupted sync, skip records last synced conversations

                transform_times(record, schema_datetimes)

                record_datetime = singer.utils.strptime_to_utc(
                    self.epoch_milliseconds_to_dt_str(
                        record[self.replication_key])
                )

                # Write the record if the parent is selected
                if is_parent_selected and record_datetime >= parent_bookmark_utc:
                    record_counter += 1
                    transformed_record = transform(record,
                                                   stream_schema,
                                                   integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                   metadata=stream_metadata)
                    # Write record if a parent is selected
                    singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                    counter.increment()
                    max_datetime = max(record_datetime, max_datetime)

                # Sync child stream, if the child is selected and if we have records greater than the child stream bookmark
                if has_child and is_child_selected and (record[self.replication_key] >= child_bookmark_ts):
                    # Long running jobs may starve the child stream extraction so skip the parent ids synced in last sync
                    # Store the parent ids so that later we can resync child records of skipped parent ids
                    if self.skip_records(record):
                        self.skipped_parent_ids.append((record.get('id'), record[self.replication_key]))
                        continue
                    state = child_stream_obj.sync_substream(record.get('id'), child_schema, child_metadata, record[self.replication_key], state)

                if record_counter == MAX_PAGE_SIZE:
                    self.write_intermediate_bookmark(state, record.get("id"), max_datetime)
                    # Reset counter
                    record_counter = 0

            bookmark_date = singer.utils.strftime(max_datetime)
            LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, counter.value))

        LOGGER.info("Stream: {}, writing final bookmark".format(self.tap_stream_id))
        self.write_bookmark(state, bookmark_date)
        return state


# pylint: disable=abstract-method
class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'
    # Boolean flag to do sync with activate version for Company and Contact Attributes streams
    sync_with_version = False

    # Disabled `unused-argument` as it causing pylint error.
    # Method which call this `sync` method is passing unused argument. So, removing argument would not work.
    # pylint: disable=too-many-arguments,unused-argument
    def sync(self,
             state: dict,
             stream_schema: dict,
             stream_metadata: dict,
             config: dict,
             transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :return: State data in the form of a dictionary
        """
        schema_datetimes = find_datetimes_in_schema(stream_schema)
        if self.sync_with_version:
            # Write activate version message
            activate_version = int(time.time() * 1000)
            activate_version_message = singer.ActivateVersionMessage(
                stream=self.tap_stream_id,
                version=activate_version)
            singer.write_message(activate_version_message)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():

                # For company and contact attributes, it is difficult to define a Primary Key
                # Thus we create a hash of some records and use it as the PK
                if self.tap_stream_id in ['company_attributes', 'contact_attributes']:
                    record = self.generate_record_hash(record)

                transform_times(record, schema_datetimes)

                transformed_record = transform(record,
                                                stream_schema,
                                                integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                metadata=stream_metadata)
                # Write records with time_extracted field
                if self.sync_with_version:
                    # Using "write_message" if the version is found. As "write_record" params do not contain "version"
                    singer.write_message(singer.RecordMessage(stream=self.tap_stream_id, record=transformed_record, version=activate_version, time_extracted=singer.utils.now()))
                else:
                    singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                counter.increment()

            LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, counter.value))

        # Write activate version after syncing
        if self.sync_with_version:
            singer.write_message(activate_version_message)

        return state


class AdminList(FullTableStream):
    """
    This stream is not replicated and only used by the Admins stream.

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-admins
    """
    tap_stream_id = 'admin_list'
    key_properties = ['id']
    to_replicate = False
    path = 'admins'
    data_key = 'admins'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        response = self.client.get(self.path)

        if not response.get(self.data_key):
            LOGGER.critical('response is empty for {} stream'.format(self.tap_stream_id))
            raise IntercomError

        # Only yield records when called by child streams
        if is_parent:
            for record in response.get(self.data_key):
                yield record.get('id')

class Admins(FullTableStream):
    """
    Retrieves admins for a workspace

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#view-an-admin
    """
    tap_stream_id = 'admins'
    key_properties = ['id']
    path = 'admins/{}'
    parent = AdminList

    def get_parent_data(self, bookmark_datetime: datetime = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :return: A list of records
        """
        # pylint: disable=not-callable
        parent = self.parent(self.client, self.catalog, self.selected_streams)
        return parent.get_records(bookmark_datetime, is_parent=True)

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))
        admins = []

        for record in self.get_parent_data():
            call_path = self.path.format(record)
            results = self.client.get(call_path)

            admins.append(results)

        yield from admins


class Companies(IncrementalStream):
    """
    Retrieves companies data using the Scroll API

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#iterating-over-all-companies
    """
    tap_stream_id = 'companies'
    key_properties = ['id']
    path = 'companies/scroll' # using Scroll API
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    data_key = 'data'

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None) -> Iterator[list]:
        scrolling = True
        params = {}
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while scrolling:
            response = self.client.get(self.path, params=params)

            if response.get(self.data_key) is None:
                LOGGER.warning('response is empty for "{}" stream'.format(self.tap_stream_id))

            records = transform_json(response, self.tap_stream_id, self.data_key)
            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(records)))

            # stop scrolling if 'data' array is empty
            if 'scroll_param' in response and response.get(self.data_key):
                scroll_param = response.get('scroll_param')
                params = {'scroll_param': scroll_param}
                LOGGER.info("Syncing next page")
            else:
                scrolling = False

            yield from records


class CompanyAttributes(FullTableStream):
    """
    Retrieves company attributes

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-data-attributes
    """
    tap_stream_id = 'company_attributes'
    key_properties = ['_sdc_record_hash']
    path = 'data_attributes'
    params = {'model': 'company'}
    data_key = 'data'
    # Sync with activate version
    # As we are preparing the hash of ['id', 'name', 'description'] and using it as the Primary Key and there are chances
    # of field value being updated, thus, on the target side, there will be a redundant entry of the same record.
    sync_with_version = True

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


class CompnaySegments(IncrementalStream):
    """
    Retrieve company segments

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-segments
    """
    tap_stream_id = 'company_segments'
    key_properties = ['id']
    path = 'segments'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {
        'type': 'company',
        'include_count': 'true'
        }
    data_key = 'segments'

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key)


class Conversations(IncrementalStream):
    """
    Retrieve conversations

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#search-for-conversations
    """
    tap_stream_id = 'conversations'
    key_properties = ['id']
    path = 'conversations/search'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'display_as': 'plaintext'}
    data_key = 'conversations'
    per_page = MAX_PAGE_SIZE
    child = 'conversation_parts'

    def set_last_processed(self, state):
        self.last_processed = singer.get_bookmark(
            state, self.tap_stream_id, "last_processed")

    def set_last_sync_started_at(self, state):
        last_sync_started_at = singer.get_bookmark(
            state, self.tap_stream_id, "last_sync_started_at")
        self.last_sync_started_at = last_sync_started_at or singer.utils.strftime(singer.utils.now())

    def skip_records(self, record):
        # If last processed id exists then check if current record id is less than last processed id
        return self.last_processed and record.get("id") <= self.last_processed

    def write_bookmark(self, state, bookmark_value):
        # Set last successful sync start time as new bookmark and delete intermitiate bookmarks
        if "last_sync_started_at" in state.get("bookmarks", {}).get(self.tap_stream_id, {}):
            bookmark_value = singer.get_bookmark(state,
                                                 self.tap_stream_id,
                                                 "last_sync_started_at")

            del state["bookmarks"][self.tap_stream_id]["last_sync_started_at"]

        if "last_processed" in state.get("bookmarks", {}).get(self.tap_stream_id, {}):
            del state["bookmarks"][self.tap_stream_id]["last_processed"]

        return singer.write_bookmark(state,
                                     self.tap_stream_id,
                                     self.replication_key,
                                     bookmark_value)

    def write_intermediate_bookmark(self, state, last_processed, bookmark_value):
        # In scenarios where sync is interrupted, we should resume from the last id processed
        state = singer.write_bookmark(state,
                                      self.tap_stream_id,
                                      "last_processed",
                                      last_processed)

        # This should be set as new bookmark once all conversation records are synced
        state = singer.write_bookmark(state,
                                      self.tap_stream_id,
                                      "last_sync_started_at",
                                      self.last_sync_started_at)
        singer.write_state(state)

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None) -> Iterator[list]:
        paging = True
        starting_after = None
        search_query = {
            'pagination': {
                'per_page': self.per_page
            },
            'query': {
                'operator': 'AND',
                'value': [
                    {
                        'operator': 'OR',
                        'value': [
                            {
                                'field': 'id',
                                'operator': '>',
                                'value': self.last_processed or ""
                            },
                            {
                                'field': 'id',
                                'operator': '=',
                                'value': self.last_processed or ""
                            }]
                    },
                    {
                        'operator': 'OR',
                        'value': [
                            {
                                'field': self.replication_key,
                                'operator': '>',
                                'value': self.dt_to_epoch_seconds(bookmark_datetime)
                            },
                            {
                                'field': self.replication_key,
                                'operator': '=',
                                'value': self.dt_to_epoch_seconds(bookmark_datetime)
                            }]
                    }]
            },
            "sort": {
                "field": "id",
                "order": "ascending"
            }
        }
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.post(self.path, json=search_query)

            if 'pages' in response and response.get('pages', {}).get('next'):
                starting_after = response.get('pages').get('next').get('starting_after')
                search_query['pagination'].update({'starting_after': starting_after})
            else:
                paging = False

            records = transform_json(response, self.tap_stream_id, self.data_key)
            LOGGER.info("Synced: {} for page: {}, records: {}".format(self.tap_stream_id, response.get('pages', {}).get('page'), len(records)))

            if is_parent:
                for record in records:
                    yield record.get('id')
            else:
                yield from records


class ConversationParts(BaseStream):
    """
    Retrieve conversation parts

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#retrieve-a-conversation
    """
    tap_stream_id = 'conversation_parts'
    key_properties = ['id']
    path = 'conversations/{}'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    parent = Conversations
    params = {'display_as': 'plaintext'}
    data_key = 'conversations'

class ContactAttributes(FullTableStream):
    """
    Retrieve contact attributes

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-data-attributes
    """
    tap_stream_id = 'contact_attributes'
    key_properties = ['_sdc_record_hash']
    path = 'data_attributes'
    params = {'model': 'contact'}
    data_key = 'data'
    # Sync with activate version
    # As we are preparing the hash of ['id', 'name', 'description'] and using it as the Primary Key and there are chances
    # of field value being updated, thus, on the target side, there will be a redundant entry of the same record.
    sync_with_version = True

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))
        paging = True
        next_page = None

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


class Contacts(IncrementalStream):
    """
    Retrieve contacts

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#search-for-contacts
    """
    tap_stream_id = 'contacts'
    key_properties = ['id']
    path = 'contacts/search'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    data_key = 'data'
    per_page = MAX_PAGE_SIZE
    # addressable_list_fields = ['tags', 'notes', 'companies']
    addressable_list_fields = ['tags', 'companies']
    to_write_intermediate_bookmark = True

    def get_addressable_list(self, contact_list: dict, stream_metadata: dict) -> dict:
        params = {
            'display_as': 'plaintext',
            'per_page': 60 # addressable_list endpoints have a different max page size in Intercom's API v2.0
        }

        for record in contact_list.get(self.data_key):
            for addressable_list_field in self.addressable_list_fields:
                # List of values from the API
                values = []
                data = record.get(addressable_list_field)
                paging = True
                next_page = None
                endpoint = data.get('url')

                # Do not do the API call to get addressable fields:
                #   If the field is not selected
                #   If we have 0 records
                #   If we have less than 10 records ie. the 'has_more' field is 'False'
                if not stream_metadata.get(('properties', addressable_list_field), {}).get('selected') or \
                    not data.get('total_count') > 0 or \
                        not data.get('has_more'):
                    continue

                while paging:
                    response = self.client.get(endpoint, url=next_page, params=params)

                    if 'pages' in response and response.get('pages', {}).get('next'):
                        next_page = response.get('pages', {}).get('next')
                        endpoint = None
                    else:
                        paging = False

                    values.extend(response.get(self.data_key, []))
                record[addressable_list_field][self.data_key] = values

        return contact_list

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None) -> Iterator[list]:
        paging = True
        starting_after = None
        search_query = {
            'pagination': {
                'per_page': self.per_page
            },
            'query': {
                'operator': 'OR',
                'value': [{
                    'field': self.replication_key,
                    'operator': '>',
                    'value': self.dt_to_epoch_seconds(bookmark_datetime)
                    },
                    {
                    'field': self.replication_key,
                    'operator': '=',
                    'value': self.dt_to_epoch_seconds(bookmark_datetime)
                    }]
                },
            'sort': {
                'field': self.replication_key,
                'order': 'ascending'
                }
        }
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.post(self.path, json=search_query)

            if 'pages' in response and response.get('pages', {}).get('next'):
                starting_after = response.get('pages').get('next').get('starting_after')
                search_query['pagination'].update({'starting_after': starting_after})
            else:
                paging = False

            # Check each contact for any records in each addressable-list object (tags, notes, companies)
            response = self.get_addressable_list(response, stream_metadata=stream_metadata)

            records = transform_json(response, self.tap_stream_id, self.data_key)
            LOGGER.info("Synced: {} for page: {}, records: {}".format(self.tap_stream_id, response.get('pages', {}).get('page'), len(records)))

            yield from records


class Segments(IncrementalStream):
    """
    Retrieve segments

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-segments
    """
    tap_stream_id = 'segments'
    key_properties = ['id']
    path = 'segments'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'include_count': 'true'}
    data_key = 'segments'

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


class Tags(FullTableStream):
    """
    Retrieve tags

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-tags-for-an-app
    """
    tap_stream_id = 'tags'
    key_properties =['id']
    path = 'tags'
    data_key = 'data'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


class Teams(FullTableStream):
    """
    Retrieve teams

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-teams
    """
    tap_stream_id = 'teams'
    key_properties = ['id']
    path = 'teams'
    data_key = 'teams'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


STREAMS = {
    "admin_list": AdminList,
    "admins": Admins,
    "companies": Companies,
    "company_attributes": CompanyAttributes,
    "company_segments": CompnaySegments,
    "conversations": Conversations,
    "conversation_parts": ConversationParts,
    "contact_attributes": ContactAttributes,
    "contacts": Contacts,
    "segments": Segments,
    "tags": Tags,
    "teams": Teams
}
