from singer.catalog import Catalog
from tap_intercom.schema import get_schemas


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

def discover():
    """
    Constructs a singer Catalog object based on the schemas and metadata.
    """
    schemas, field_metadata = get_schemas()
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
