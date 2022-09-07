
import singer
from singer import Transformer, metadata


from tap_intercom.client import IntercomClient
from tap_intercom.streams import STREAMS, Admins

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

def get_streams_to_sync(catalog, selected_streams, selected_stream_names):
    """
        Get streams to sync ie. selected streams and parent stream if child stream or parent stream is selected
    """
    streams_to_sync = []

    for stream in selected_streams:
        stream_obj = STREAMS.get(stream.tap_stream_id)
        # If the stream is independant stream, then add to sync
        if not stream_obj.parent:
            streams_to_sync.append(stream)
        # If the stream is child stream, then add parent stream to sync
        elif stream_obj.parent:
            # The AdminList and Admin are parent-child streams, the admins is written in the catalog but the admin_list is not.
            # The purpose of admin_list is to return a list of admins through which individual admins will be synced.
            if stream_obj == Admins:
                streams_to_sync.append(catalog.get_stream(stream_obj.tap_stream_id))
            elif stream_obj.parent.tap_stream_id not in selected_stream_names:
                streams_to_sync.append(catalog.get_stream(stream_obj.parent.tap_stream_id))

    return streams_to_sync

def sync(config, state, catalog):
    """ Sync data from tap source """

    access_token = config.get('access_token')
    client = IntercomClient(access_token, config.get('request_timeout'), config.get('user_agent')) # pass request_timeout parameter from config

    # Translate state to the new format with replication key in the state
    state = translate_state(state)

    selected_stream_names = []
    selected_streams = list(catalog.get_selected_streams(state))
    for stream in selected_streams:
        selected_stream_names.append(stream.tap_stream_id)

    with Transformer() as transformer:
        for stream in get_streams_to_sync(catalog, selected_streams, selected_stream_names):
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client, catalog, selected_stream_names)
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

            state = stream_obj.sync(state, stream_schema, stream_metadata, config, transformer)
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
