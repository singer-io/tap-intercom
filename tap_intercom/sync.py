
import singer
from singer import Transformer, metadata


from tap_intercom.client import IntercomClient
from tap_intercom.streams import STREAMS

LOGGER = singer.get_logger()

def translate_state(state):
    """
    Tap was used to write bookmark using custom get_bookmark and write_bookmark methods,
    in which case the state looked like the following format.
    {
        "bookmarks": {
            "company_segments": "2021-12-20T21:30:35.000000Z"
        }
    }

    The tap now uses get_bookmark and write_bookmark methods of singer library,
    which expects state in a new format with replication keys.
    So this function should be called at beginning of each run to ensure the state is translated to a new format.
    {
        "bookmarks": {
            "company_segments": {
                "updated_at": "2021-12-20T21:30:35.000000Z"
            }
        }
    }
    """

    # Iterate over all streams available in state
    for stream, bookmark in state.get("bookmarks", {}).items():
        # If bookmark is directly present without replication_key(old format)
        # then add replication key at inner level
        if isinstance(bookmark, str):
            # Stream `companies` is changed from incremental to full_table
            # Stream `conversation_parts` is changed from full_table to incremental and again full_table
            # so adding replication key used at incremental time to keep consistency.
            replication_key = 'updated_at' if stream in ["companies", "conversation_parts"] else STREAMS[stream].replication_key
            state["bookmarks"][stream] = {replication_key : bookmark}

    return state

def sync(config, state, catalog):
    """ Sync data from tap source """

    access_token = config.get('access_token')
    client = IntercomClient(access_token, config.get('request_timeout'), config.get('user_agent')) # pass request_timeout parameter from config

    # Translate state to new format with replication key in state
    state = translate_state(state)

    # `tap_state` will preserve state passed in sync mode and
    # `state` will be updated and written to output based on current sync
    tap_state = state.copy()

    selected_streams = []
    # Add parent-stream to selected_streams if child-stream is selected
    # but parent-stream is not selected
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.tap_stream_id)
        parent_stream = STREAMS[stream.tap_stream_id].parent # Get parent stream
        # If stream have parent stream and not selected then add it to selected_stream
        if parent_stream and parent_stream.tap_stream_id not in selected_streams:
            selected_streams.append(parent_stream.tap_stream_id)

    LOGGER.info('selected_streams: {}'.format(selected_streams))

    with Transformer() as transformer:
        # Iterate over selected_streams
        for stream_name in selected_streams:
            stream = catalog.get_stream(stream_name)
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client)
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info('Starting sync for stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            state = stream_obj.sync(tap_state, state, stream_schema, stream_metadata, config, transformer)
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
