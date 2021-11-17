"""
This module defines the stream classes and their individual sync logic.
"""


import datetime
from typing import Iterator

import singer
from singer import Transformer, metrics, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING
from singer.transform import transform

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

    def __init__(self, client: IntercomClient):
        self.client = client

    def get_records(self, bookmark_datetime: datetime = None, is_parent: bool = False) -> list:
        """
        Returns a list of records for that stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require "
                                  "`get_records` implementation")

    def get_parent_data(self, bookmark_datetime: datetime = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :return: A list of records
        """
        # pylint: disable=not-callable
        parent = self.parent(self.client)
        return parent.get_records(bookmark_datetime, is_parent=True)

    @staticmethod
    def epoch_milliseconds_to_dt_str(timestamp: float) -> str:
        dt_object = datetime.datetime.fromtimestamp(timestamp / 1000.0)
        return dt_object.isoformat()

    @staticmethod
    def dt_to_epoch_seconds(dt_object: datetime) -> float:
        return datetime.datetime.timestamp(dt_object)


# pylint: disable=abstract-method
class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'

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
        start_date = singer.get_bookmark(state,
                                         self.tap_stream_id,
                                         self.replication_key,
                                         config['start_date'])

        bookmark_datetime = singer.utils.strptime_to_utc(start_date)
        max_datetime = bookmark_datetime

        schema_datetimes = find_datetimes_in_schema(stream_schema)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(bookmark_datetime):
                transform_times(record, schema_datetimes)

                record_datetime = singer.utils.strptime_to_utc(
                    self.epoch_milliseconds_to_dt_str(
                        record[self.replication_key])
                    )

                if record_datetime >= bookmark_datetime:
                    transformed_record = transform(record,
                                                    stream_schema,
                                                    integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                    metadata=stream_metadata)
                    singer.write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_datetime = max(record_datetime, max_datetime)
            bookmark_date = singer.utils.strftime(max_datetime)

        state = singer.write_bookmark(state,
                                      self.tap_stream_id,
                                      self.replication_key,
                                      bookmark_date)
        return state


# pylint: disable=abstract-method
class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'

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

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transform_times(record, schema_datetimes)

                transformed_record = transform(record,
                                                stream_schema,
                                                integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                metadata=stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()

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

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        admins = []

        for record in self.get_parent_data():
            call_path = self.path.format(record)
            results = self.client.get(call_path)

            admins.append(results)

        yield from admins


class Companies(FullTableStream):
    """
    Retrieves companies data using the Scroll API

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#iterating-over-all-companies
    """
    tap_stream_id = 'companies'
    key_properties = ['id']
    path = 'companies/scroll' # using Scroll API
    data_key = 'data'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        scrolling = True
        params = {}

        while scrolling:
            response = self.client.get(self.path, params=params)

            if response.get(self.data_key) is None:
                LOGGER.warning('response is empty for "{}" stream'.format(self.tap_stream_id))

            # stop scrolling if 'data' array is empty
            if 'scroll_param' in response and response.get(self.data_key):
                scroll_param = response.get('scroll_param')
                params = {'scroll_param': scroll_param}
            else:
                scrolling = False

            records = transform_json(response, self.tap_stream_id, self.data_key)

            yield from records


class CompanyAttributes(FullTableStream):
    """
    Retrieves company attributes

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-data-attributes
    """
    tap_stream_id = 'company_attributes'
    key_properties = ['name']
    path = 'data_attributes'
    params = {'model': 'company'}
    data_key = 'data'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
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

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
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

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        starting_after = None
        search_query = {
            'pagination': {
                'per_page': self.per_page
            },
            "query": {
                "field": self.replication_key,
                "operator": ">",
                "value": self.dt_to_epoch_seconds(bookmark_datetime)
            },
            "sort": {
                "field": self.replication_key,
                "order": "ascending"
            }
        }

        while paging:
            response = self.client.post(self.path, json=search_query)

            if 'pages' in response and response.get('pages', {}).get('next'):
                starting_after = response.get('pages').get('next').get('starting_after')
                search_query['pagination'].update({'starting_after': starting_after})
            else:
                paging = False

            records = transform_json(response, self.tap_stream_id, self.data_key)

            if is_parent:
                for record in records:
                    yield record.get('id')
            else:
                yield from records


class ConversationParts(Conversations):
    """
    Retrieve conversation parts

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#retrieve-a-conversation
    """
    tap_stream_id = 'conversation_parts'
    key_properties = ['id']
    path = 'conversations/{}'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    parent = Conversations
    params = {'display_as': 'plaintext'}
    data_key = 'conversations'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        results = []
        for record in self.get_parent_data(bookmark_datetime):
            call_path = self.path.format(record)
            response = self.client.get(call_path, params=self.params)

            results.append(response)

        data_for_transform = {self.data_key: results}

        transformed_records = transform_json(data_for_transform, self.tap_stream_id, self.data_key)

        yield from transformed_records


class ContactAttributes(FullTableStream):
    """
    Retrieve contact attributes

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-data-attributes
    """
    tap_stream_id = 'contact_attributes'
    key_properties = ['name']
    path = 'data_attributes'
    params = {'model': 'contact'}
    data_key = 'data'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
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


    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
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

        while paging:
            response = self.client.post(self.path, json=search_query)

            if 'pages' in response and response.get('pages', {}).get('next'):
                starting_after = response.get('pages').get('next').get('starting_after')
                search_query['pagination'].update({'starting_after': starting_after})
            else:
                paging = False

            records = transform_json(response, self.tap_stream_id, self.data_key)

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

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
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

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
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

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
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
