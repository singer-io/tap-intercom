
import singer
from singer import Transformer, metadata


<<<<<<< HEAD
def get_bookmark(state, stream, bookmark_field, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return singer.get_bookmark(state, stream, bookmark_field, default)
=======
from tap_intercom.client import IntercomClient
from tap_intercom.streams import STREAMS
>>>>>>> f32016c (canges to sync.py)

LOGGER = singer.get_logger()

<<<<<<< HEAD
def write_bookmark(state, stream, bookmark_field, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = {bookmark_field: value}
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)
=======
def sync(config, state, catalog):
    """ Sync data from tap source """
>>>>>>> f32016c (canges to sync.py)

    access_token = config.get('access_token')
    client = IntercomClient(access_token, config.get('user_agent'))

    with Transformer() as transformer:
<<<<<<< HEAD
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    bookmark_type=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    last_integer=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    schema_datetimes = find_datetimes_in_schema(schema)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id


            # API returns inconsistent epoch representations sec AND millis
            # in addition to utc timestamps. Normalize to milli for transformer
            transform_times(record, schema_datetimes)

            # Transform record for Singer.io
            with Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) \
                as transformer:
                try:
                    transformed_record = transformer.transform(
                        record,
                        schema,
                        stream_metadata)
                except Exception as err:
                    LOGGER.error('Transformer Error: {}'.format(err))
                    LOGGER.error('Stream: {}, record: {}'.format(stream_name, record))
                    raise
                # Reset max_bookmark_value to new value if higher
                if transformed_record.get(bookmark_field):
                    if max_bookmark_value is None or \
                        transformed_record[bookmark_field] > transform_datetime(max_bookmark_value):
                        max_bookmark_value = transformed_record[bookmark_field]
                        # LOGGER.info('{}, increase max_bookkmark_value: {}'.format(stream_name, max_bookmark_value))

                if bookmark_field and (bookmark_field in transformed_record):
                    if bookmark_type == 'integer':
                        # Keep only records whose bookmark is after the last_integer
                        if transformed_record[bookmark_field] >= last_integer:
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                    elif bookmark_type == 'datetime':
                        last_dttm = transform_datetime(last_datetime)
                        bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, counter.value


# Sync a specific parent or child endpoint.
def sync_endpoint(client, #pylint: disable=too-many-branches
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  endpoint_config,
                  static_params,
                  bookmark_query_field=None,
                  bookmark_field=None,
                  bookmark_type=None,
                  data_key=None,
                  id_fields=None,
                  selected_streams=None,
                  replication_ind=None,
                  parent=None,
                  parent_id=None):

    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = None
    last_integer = None
    max_bookmark_value = None
    if bookmark_type == 'integer':
        last_integer = get_bookmark(state, stream_name, bookmark_field, 0)
        max_bookmark_value = last_integer
    else:
        last_datetime = get_bookmark(state, stream_name, bookmark_field, start_date)
        max_bookmark_value = last_datetime
        LOGGER.info('{}, initial max_bookmark_value {}'.format(stream_name, max_bookmark_value))
        max_bookmark_dttm = strptime_to_utc(last_datetime)
        max_bookmark_int = int(time.mktime(max_bookmark_dttm.timetuple()))
        now_int = int(time.time())
        updated_since_sec = now_int - max_bookmark_int
        updated_since_days = math.ceil(updated_since_sec/(24 * 60 * 60))

    # pagination: loop thru all pages of data using next_url (if not None)
    page = 1
    offset = 0
    # Default per_page limit is 50, max is 60
    limit = endpoint_config.get('batch_size', 60)
    total_records = 0

    # Check scroll_type to determine if to use Scroll API
    #   scroll_types: always, never.
    #   Endpoints:
    #       always: customers
    #       never: all others
    # Scroll API: https://developers.intercom.io/reference?_ga=2.237132992.1579857338.1569387987-1032864292.1569297580#iterating-over-all-users
    scroll_type = endpoint_config.get('scroll_type', 'never')

    # Check whether the endpoint supports a cursor
    # https://developers.intercom.com/intercom-api-reference/reference#pagination-cursor
    cursor = endpoint_config.get('cursor', False)
    search = endpoint_config.get('search', False)
=======
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client)
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)
>>>>>>> f32016c (canges to sync.py)

            LOGGER.info('Starting sync for stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            state = stream_obj.sync(state, stream_schema, stream_metadata, config, transformer)
            singer.write_state(state)

<<<<<<< HEAD
        # API request data
        data = {}
        data = client.perform(
            method=endpoint_config.get('method'),
            url=next_url,
            path=path,
            params=querystring,
            endpoint=stream_name,
            json=request_body)

        # LOGGER.info('data = {}'.format(data)) # TESTING, comment out

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()
        if not data or data is None or data == {}:
            break # No data results

        # Transform data with transform_json from transform.py
        # The data_key identifies the array/list of records below the <root> element.
        # SINGLE RECORD data results appear as dictionary.
        # MULTIPLE RECORD data results appear as an array-list under the data_key.
        # The following code converts ALL results to an array-list and transforms data.
        transformed_data = [] # initialize the record list
        data_list = []
        data_dict = {}
        if isinstance(data, list) and not data_key in data:
            data_list = data
            data_dict[data_key] = data_list
            transformed_data = transform_json(data_dict, stream_name, data_key)
        elif isinstance(data, dict) and not data_key in data:
            data_list.append(data)
            data_dict[data_key] = data_list
            transformed_data = transform_json(data_dict, stream_name, data_key)
        else:
            transformed_data = transform_json(data, stream_name, data_key)
        # LOGGER.info('transformed_data = {}'.format(transformed_data))  # TESTING, comment out
        if not transformed_data or transformed_data is None:
            if parent_id is None:
                LOGGER.info('Stream: {}, No transformed data for data = {}'.format(
                    stream_name, data))
            break # No data results
        # Verify key id_fields are present
        rec_count = 0
        for record in transformed_data:
            for key in id_fields:
                if not record.get(key):
                    LOGGER.info('Stream: {}, Missing key {} in record: {}'.format(
                        stream_name, key, record))
                    raise RuntimeError
            rec_count = rec_count + 1

        # Process records and get the max_bookmark_value and record_count for the set of records
        if replication_ind:
            max_bookmark_value, record_count = process_records(
                catalog=catalog,
                stream_name=stream_name,
                records=transformed_data,
                time_extracted=time_extracted,
                bookmark_field=bookmark_field,
                bookmark_type=bookmark_type,
                max_bookmark_value=max_bookmark_value,
                last_datetime=last_datetime,
                last_integer=last_integer,
                parent=parent,
                parent_id=parent_id)
            LOGGER.info('Stream {}, batch processed {} records'.format(
                stream_name, record_count))
        else:
            record_count = 0

        # Loop thru parent batch records for each children objects (if should stream)
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                if child_stream_name in selected_streams:
                    child_replication_ind = child_endpoint_config.get('replication_ind', True)
                    if child_replication_ind:
                        write_schema(catalog, child_stream_name)
                        child_selected_fields = get_selected_fields(catalog, child_stream_name)
                        LOGGER.info('Stream: {}, selected_fields: {}'.format(
                            child_stream_name, child_selected_fields))
                        total_child_records = 0
                        # For each parent record
                        for record in transformed_data:
                            i = 0
                            # Set parent_id
                            for id_field in id_fields:
                                if i == 0:
                                    parent_id_field = id_field
                                if id_field == 'id':
                                    parent_id_field = id_field
                                i = i + 1
                            parent_id = record.get(parent_id_field)

                            # sync_endpoint for child
                            LOGGER.info('Syncing: {}, parent_stream: {}, parent_id: {}'.format(
                                child_stream_name,
                                stream_name,
                                parent_id))
                            child_path = child_endpoint_config.get('path', child_stream_name).format(
                                str(parent_id))
                            child_bookmark_field = next(iter(child_endpoint_config.get(
                                'replication_keys', [])), None)
                            child_total_records = sync_endpoint(
                                client=client,
                                catalog=catalog,
                                state=state,
                                start_date=start_date,
                                stream_name=child_stream_name,
                                path=child_path,
                                endpoint_config=child_endpoint_config,
                                static_params=child_endpoint_config.get('params', {}),
                                bookmark_query_field=child_endpoint_config.get(
                                    'bookmark_query_field', None),
                                bookmark_field=child_bookmark_field,
                                bookmark_type=child_endpoint_config.get('bookmark_type', None),
                                data_key=child_endpoint_config.get('data_key', child_stream_name),
                                id_fields=child_endpoint_config.get('key_properties'),
                                selected_streams=selected_streams,
                                replication_ind=child_replication_ind,
                                parent=child_endpoint_config.get('parent'),
                                parent_id=parent_id)
                            LOGGER.info('Synced: {}, parent_id: {}, records: {}'.format(
                                child_stream_name,
                                parent_id,
                                child_total_records))
                            total_child_records = total_child_records + child_total_records
                        LOGGER.info('Parent Stream: {}, Child Stream: {}, FINISHED PARENT BATCH'.format(
                            stream_name, child_stream_name))
                        LOGGER.info('Synced: {}, total_records: {}'.format(
                            child_stream_name,
                            total_child_records))

        # set total_records and next_url for pagination
        total_records = total_records + record_count
        if is_scrolling:
            scroll_param = data.get('scroll_param')
            if not scroll_param:
                break
            next_url = '{}/{}/scroll?scroll_param={}'.format(client.base_url, path, scroll_param)
        elif cursor:
            pagination = data.get('pages', {}).get('next', {})
            starting_after = pagination.get('starting_after', None)
            next_url = '{}/{}?starting_after={}'.format(client.base_url, path, starting_after)
        elif search:
            pagination = data.get('pages', {}).get('next', {})
            starting_after = pagination.get('starting_after', None)
            # Subsequent search queries require starting_after
            if starting_after:
                request_body = build_query(search_query, max_bookmark_int, starting_after)
            else:
                next_url = None
        else:
            next_url = data.get('pages', {}).get('next', None)

        # Update the state with the max_bookmark_value for non-scrolling
        if bookmark_field and not is_scrolling:
            write_bookmark(state, stream_name, bookmark_field, max_bookmark_value)

        # to_rec: to record; ending record for the batch page
        to_rec = offset + rec_count
        LOGGER.info('Synced Stream: {}, page: {}, records: {} to {}'.format(
            stream_name,
            page,
            offset,
            to_rec))
        # Pagination: increment the offset by the limit (batch-size) and page
        offset = offset + rec_count
        page = page + 1
        i = i + 1

    # Return total_records across all pages
    LOGGER.info('Synced Stream: {}, pages: {}, total records: {}'.format(
        stream_name,
        page - 1,
        total_records))

    # Update the state with the max_bookmark_value for non-scrolling
    if bookmark_field and is_scrolling:
        write_bookmark(state, stream_name, bookmark_field, max_bookmark_value)

    return total_records


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
=======
    state = singer.set_currently_syncing(state, None)
>>>>>>> f32016c (canges to sync.py)
    singer.write_state(state)
