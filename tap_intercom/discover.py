import singer
from singer.catalog import Catalog

from tap_intercom.client import IntercomClient, IntercomForbiddenError
from tap_intercom.schema import get_schemas
from tap_intercom.streams import STREAMS

LOGGER = singer.get_logger()


def _get_key_properties_from_meta(schema_meta: list) -> str:
    """
    Gets the table-key-properties from the schema metadata.
    """
    return schema_meta[0].get('metadata').get('table-key-properties')


def _get_replication_method_from_meta(schema_meta: list) -> str:
    """
    Gets the forced-replication-method from the schema metadata.
    """
    return schema_meta[0].get('metadata').get('forced-replication-method')


def _get_replication_key_from_meta(schema_meta: list) -> str:
    """
    Gets the valid-replication-keys from the schema metadata.
    """
    if _get_replication_method_from_meta(schema_meta) == 'INCREMENTAL':
        return schema_meta[0].get('metadata').get('valid-replication-keys')[0]
    return None


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> list:
    """
    Remove child streams from the catalog whose parent stream was excluded.

    A child is pruned only when its parent is a replicable stream
    (to_replicate=True) that is absent from schemas, meaning it was removed
    due to an access failure.  Children of non-replicable helper streams
    (e.g. AdminList) are intentionally skipped.

    Mutates schemas and field_metadata in place and returns the list of
    pruned child stream names so callers can include them.
    """
    pruned = []
    for name, stream_cls in list(STREAMS.items()):
        parent = stream_cls.parent
        if (name in schemas
                and parent is not None
                and getattr(parent, 'to_replicate', True)
                and parent.tap_stream_id not in schemas):
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent "
                "stream '%s' is not accessible.",
                name, parent.tap_stream_id,
            )
            schemas.pop(name)
            field_metadata.pop(name)
            pruned.append(name)
    return pruned


def _apply_access_checks(
    client: IntercomClient, schemas: dict, field_metadata: dict
) -> None:
    """
    Probe each stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.

    Independent (non-child) streams are probed first.  Child streams whose
    parent has already been found inaccessible are excluded without
    making any additional API calls.

    Raises IntercomForbiddenError if no streams remain in the catalog after
    access checks, since discovery would otherwise silently produce a
    usable-looking but empty catalog.
    """
    inaccessible_streams = []

    # Probe independent streams.
    for stream_name, stream_cls in STREAMS.items():
        if stream_name not in schemas:
            continue
        parent = stream_cls.parent
        has_replicable_parent = parent is not None and getattr(parent, 'to_replicate', True)
        if has_replicable_parent:
            continue
        if not stream_cls(client=client).check_access():
            inaccessible_streams.append(stream_name)

    # Probe child streams, skipping those whose parent is already denied.
    for stream_name, stream_cls in STREAMS.items():
        if stream_name not in schemas:
            continue
        parent = stream_cls.parent
        has_replicable_parent = parent is not None and getattr(parent, 'to_replicate', True)
        if not has_replicable_parent:
            continue
        if parent.tap_stream_id in inaccessible_streams:
            # Parent already denied — skip probing to avoid a duplicate API call.
            inaccessible_streams.append(stream_name)
            continue
        if not stream_cls(client=client).check_access():
            inaccessible_streams.append(stream_name)

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    pruned_children = _prune_inaccessible_children(schemas, field_metadata)
    inaccessible_streams.extend(pruned_children)

    if not schemas:
        raise IntercomForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "Unauthorised streams excluded from catalog: %s",
            ", ".join(inaccessible_streams),
        )


def discover(client: IntercomClient) -> Catalog:
    """
    Constructs a singer Catalog object based on the schemas and metadata.

    Access to each stream is verified using the provided client; streams the
    credentials cannot read are excluded from the returned catalog.
    """
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)

    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = field_metadata[schema_name]

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'key_properties': _get_key_properties_from_meta(schema_meta),
            'replication_method': _get_replication_method_from_meta(schema_meta),
            'replication_key': _get_replication_key_from_meta(schema_meta),
            'metadata': schema_meta
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({'streams': streams})
