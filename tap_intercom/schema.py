import json
import os

from singer import get_logger, metadata

from tap_intercom.client import IntercomClient, IntercomError, IntercomScrollExistsError
from tap_intercom.streams import STREAMS

LOGGER = get_logger()

# Reference:
# https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def prune_inaccessible_children(schemas, field_metadata, inaccessible_streams):
    """ Function to check the schemas having any child stream whose parent is not accessible
    """
    for stream_name, stream_obj in STREAMS.items():
        if stream_obj.parent and stream_obj.parent.tap_stream_id in inaccessible_streams:
            if stream_name in schemas:
                del schemas[stream_name]
            if stream_name in field_metadata:
                del field_metadata[stream_name]


def check_stream_access(client: IntercomClient, stream_obj):
    """
    Checks if the stream is accessible with the provided credentials by making a test API call to the endpoint.
    Returns True if the stream is accessible, False if not (catches IntercomError internally and logs it).
    """
    stream_name = stream_obj.tap_stream_id

    # Get the specific details from the stream object.
    path = stream_obj.path
    params = stream_obj.params if hasattr(stream_obj, 'params') else {}
    json_body = {}
    # Default to GET if probe_http_method is not defined in the stream object
    probe_http_method = getattr(stream_obj, 'probe_http_method', 'GET').upper()

    has_parent = stream_obj.parent is not None
    if has_parent:
        # If the stream has a parent stream, then it's access depends on the parent stream's access.
        parent_stream_name = stream_obj.parent.tap_stream_id
        LOGGER.info("Stream {} has parent stream {}. Access depends on parent stream.".format(stream_name, parent_stream_name))
        return True

    if probe_http_method == "POST":  # Update the json with the probe_search_query
        json_body = getattr(stream_obj, 'probe_search_query', {})

    try:
        LOGGER.info("Checking access for stream: {}".format(stream_name))
        client.probe_stream(path, http_method=probe_http_method, params=params, json=json_body)
        LOGGER.info("Stream {} is accessible".format(stream_name))
        return True
    except IntercomScrollExistsError:
        # A scroll_exists error means a prior scroll session is still open for this workspace.
        # The endpoint itself is reachable and the stream is accessible — treat as success.
        LOGGER.info("Stream {} is accessible (scroll already exists for workspace).".format(stream_name))
        return True
    except IntercomError as e:
        LOGGER.error("Stream {} is not accessible. Error: {}".format(stream_name, str(e)))
        return False


def get_schemas(client: IntercomClient):
    """
    Loads the schemas defined for the tap.

    This function iterates through the STREAMS dictionary which contains
    a mapping of the stream name and its corresponding class and loads
    the matching schema file from the schemas directory.
    """
    schemas = {}
    field_metadata = {}

    inaccessible_streams = []

    for stream_name, stream_object in STREAMS.items():
        replication_ind = stream_object.to_replicate
        if replication_ind:
            # Check if the stream is a child streams and it's parent stream is inaccessible,
            # then mark the stream as inaccessible without checking access for the child stream.
            if stream_object.parent and stream_object.parent.tap_stream_id in inaccessible_streams:
                inaccessible_streams.append(stream_name)
                LOGGER.warning("Stream {} is a child stream and its parent stream {} is inaccessible,"
                               " hence marking stream as inaccessible".format(stream_name, stream_object.parent.tap_stream_id))
                continue

            # Check stream access
            if not check_stream_access(client, stream_object):
                inaccessible_streams.append(stream_name)
                continue

            schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
            with open(schema_path) as file:
                schema = json.load(file)
            schemas[stream_name] = schema

            # Documentation:
            # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
            # Reference:
            # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
            mdata = metadata.get_standard_metadata(
                schema=schema,
                key_properties=stream_object.key_properties,
                valid_replication_keys=stream_object.valid_replication_keys,
                replication_method=stream_object.replication_method
            )

            mdata = metadata.to_map(mdata)

            if stream_object.replication_key:
                mdata = metadata.write(
                    mdata,
                    ('properties', stream_object.replication_key),
                    'inclusion',
                    'automatic')

            mdata = metadata.to_list(mdata)

            field_metadata[stream_name] = mdata

    # check if any child stream whose parent is not accessible is still in schemas
    prune_inaccessible_children(schemas, field_metadata, inaccessible_streams)

    if not schemas:  # Raise error if none of the streams were added in schemas.
        error_msg = "No accessible streams found with the provided credentials. Please check the configuration."
        LOGGER.error(error_msg)
        raise IntercomError(error_msg)

    if inaccessible_streams:
        LOGGER.warning("The following streams were found to be inaccessible and have been excluded: {}".format(", ".join(inaccessible_streams)))

    return schemas, field_metadata
