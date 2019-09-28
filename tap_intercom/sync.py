import time
import math
import singer
from singer import metrics, metadata, Transformer, utils, UNIX_SECONDS_INTEGER_DATETIME_PARSING
from singer.utils import strptime_to_utc
from tap_intercom.transform import transform_json
from tap_intercom.streams import STREAMS

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


# def transform_datetime(this_dttm):
#     with Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) \
#         as transformer:
#         new_dttm = transformer._transform_datetime(this_dttm)
#     return new_dttm
def transform_datetime(this_dttm):
    with Transformer() as transformer:
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

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer(integer_datetime_fmt=UNIX_SECONDS_INTEGER_DATETIME_PARSING) \
                as transformer:
                transformed_record = transformer.transform(
                    record,
                    schema,
                    stream_metadata)
                # Reset max_bookmark_value to new value if higher
                if transformed_record.get(bookmark_field):
                    if max_bookmark_value is None or \
                        transformed_record[bookmark_field] > transform_datetime(max_bookmark_value):
                        max_bookmark_value = transformed_record[bookmark_field]

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
                  scroll_ind=None,
                  selected_streams=None,
                  parent=None,
                  parent_id=None):

    # Get the latest bookmark for the stream and set the last_integer/datetime
    last_datetime = None
    last_integer = None
    max_bookmark_value = None
    if bookmark_type == 'integer':
        last_integer = get_bookmark(state, stream_name, 0)
        max_bookmark_value = last_integer
    else:
        last_datetime = get_bookmark(state, stream_name, start_date)
        max_bookmark_value = last_datetime
        max_bookmark_dttm = strptime_to_utc(last_datetime)
        max_bookmark_int = int(time.mktime(max_bookmark_dttm.timetuple()))
        now_int = int(time.time())
        updated_since_sec = now_int - max_bookmark_int
        updated_since_days = math.ceil(updated_since_sec/(24 * 60 * 60))

    # pagination: loop thru all pages of data using next_url (if not None)
    page = 1
    offset = 0
    limit = 60 # Default per_page limit is 50, max for Users is 60
    total_records = 0

    # If scroll_ind = true and historical sync, then use Scroll API
    # Scroll API: https://developers.intercom.io/reference?_ga=2.237132992.1579857338.1569387987-1032864292.1569297580#iterating-over-all-users
    if scroll_ind and max_bookmark_value[0:10] == start_date[0:10]:
        LOGGER.info('Stream: {}, Historical Sync, Using Scoll API'.format(stream_name))
        is_scrolling = True
        next_url = '{}/{}/scroll'.format(client.base_url, path)
        params = {}
    else:
        is_scrolling = False
        next_url = '{}/{}'.format(client.base_url, path)
        params = {
            'page': page,
            'per_page': limit,
            **static_params # adds in endpoint specific, sort, filter params
        }

    while next_url is not None:
        # Need URL querystring for 1st page; subsequent pages provided by next_url
        # querystring: Squash query params into string
        if page == 1 and not is_scrolling:
            if bookmark_query_field:
                if bookmark_type == 'datetime':
                    params[bookmark_query_field] = updated_since_days
                elif bookmark_type == 'integer':
                    params[bookmark_query_field] = last_integer
            if params != {}:
                querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
        else:
            querystring = None
        LOGGER.info('URL for Stream {}: {}{}'.format(
            stream_name,
            next_url,
            '?{}'.format(querystring) if querystring else ''))

        # API request data
        data = {}
        data = client.get(
            url=next_url,
            path=path,
            params=querystring,
            endpoint=stream_name)

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()
        if not data or data is None or data == {}:
            return total_records # No data results

        # Transform data with transform_json from transform.py
        # The data_key identifies the array/list of records below the <root> element
        # LOGGER.info('data = {}'.format(data)) # TESTING, comment out
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
            LOGGER.info('No transformed data for data = {}'.format(data))
            return total_records # No data results
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

            # Loop thru parent batch records for each children objects (if should stream)
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                if child_stream_name in selected_streams:
                    write_schema(catalog, child_stream_name)
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
                            scroll_ind=child_endpoint_config.get('scroll_ind', False),
                            selected_streams=selected_streams,
                            parent=child_endpoint_config.get('parent'),
                            parent_id=parent_id)
                        LOGGER.info('Synced: {}, parent_id: {}, total_records: {}'.format(
                            child_stream_name,
                            parent_id,
                            child_total_records))

        # set total_records and next_url for pagination
        total_count = data.get('total_count', 0)
        if total_count == 0:
            total_records = total_records + rec_count
        else:
            total_records = total_count
        next_url = None
        if is_scrolling:
            scroll_param = data.get('scroll_param')
            if not scroll_param:
                return total_records
            next_url = '{}/{}/scroll?scroll_param={}'.format(client.base_url, path, scroll_param)
        else:
            next_url = data.get('pages', {}).get('next', None)

        # Update the state with the max_bookmark_value for the stream
        if bookmark_field:
            write_bookmark(state, stream_name, max_bookmark_value)

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

    # Return total_records across all pages
    LOGGER.info('Synced Stream: {}, pages: {}, total records: {}'.format(
        stream_name,
        page - 1,
        total_records))
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
    singer.write_state(state)


def sync(client, config, catalog, state):
    if 'start_date' in config:
        start_date = config['start_date']

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # Loop through selected_streams
    for stream_name in selected_streams:
        LOGGER.info('START Syncing: {}'.format(stream_name))
        update_currently_syncing(state, stream_name)
        endpoint_config = STREAMS[stream_name]
        path = endpoint_config.get('path', stream_name)
        bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
        write_schema(catalog, stream_name)
        total_records = sync_endpoint(
            client=client,
            catalog=catalog,
            state=state,
            start_date=start_date,
            stream_name=stream_name,
            path=path,
            endpoint_config=endpoint_config,
            static_params=endpoint_config.get('params', {}),
            bookmark_query_field=endpoint_config.get('bookmark_query_field', None),
            bookmark_field=bookmark_field,
            bookmark_type=endpoint_config.get('bookmark_type', None),
            data_key=endpoint_config.get('data_key', stream_name),
            id_fields=endpoint_config.get('key_properties'),
            scroll_ind=endpoint_config.get('scroll_ind', False),
            selected_streams=selected_streams)

        update_currently_syncing(state, None)
        LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
            stream_name,
            total_records))
