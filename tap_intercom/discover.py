from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_intercom.schema import get_schemas
from tap_intercom.streams import flatten_streams

def discover():
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    flat_streams = flatten_streams()
    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = metadata.to_map(field_metadata[stream_name])

        stream = flat_streams.get(stream_name, {})
        if stream.get('replication_method') == 'INCREMENTAL':
            for field_name in stream.get('replication_keys'):
                metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=stream.get('key_properties', None),
            schema=schema,
            metadata=metadata.to_list(mdata)
        ))

    return catalog
