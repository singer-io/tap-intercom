
import singer
from singer import Transformer, metadata


from tap_intercom.client import IntercomClient
from tap_intercom.streams import STREAMS, CHILD_STREAMS

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
            replication_key = STREAMS[stream].replication_key
            state["bookmarks"][stream] = {replication_key : bookmark}

    return state

def sync(config, state, catalog):
    """ Sync data from tap source """

    access_token = config.get('access_token')
    client = IntercomClient(access_token, config.get('request_timeout'), config.get('user_agent')) # pass request_timeout parameter from config

    # Translate state to new format with replication key in state
    state = translate_state(state)

    selected_stream_names = []
    selected_streams = list(catalog.get_selected_streams(state))
    for stream in selected_streams:
        selected_stream_names.append(stream.stream)

    # loop over the parent-child stream and add parent stream to sync if child stream is selected and parent stream is not selected
    for parent_stream, child_stream in CHILD_STREAMS.items():
        if child_stream in selected_stream_names and parent_stream not in selected_stream_names:
            selected_streams.append(catalog.get_stream(parent_stream))

    with Transformer() as transformer:
        for stream in selected_streams:
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client, catalog, selected_stream_names)
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            if stream.tap_stream_id in list(CHILD_STREAMS.values()):
                continue

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

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
