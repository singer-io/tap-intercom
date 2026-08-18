import unittest

from tap_intercom.schema import get_schemas
from tap_intercom.streams import STREAMS


class TestGetSchemas(unittest.TestCase):
    """
    Tests for get_schemas() — a pure schema loader with no access logic.
    """

    def test_all_replicable_streams_included(self):
        """All streams with to_replicate=True appear in schemas."""
        schemas, field_metadata = get_schemas()
        expected = {
            name for name, cls in STREAMS.items() if cls.to_replicate
        }
        self.assertEqual(set(schemas.keys()), expected)
        self.assertEqual(set(field_metadata.keys()), expected)

    def test_non_replicable_streams_excluded(self):
        """Streams with to_replicate=False (e.g. admin_list) are absent."""
        schemas, _ = get_schemas()
        non_replicable = [
            name for name, cls in STREAMS.items() if not cls.to_replicate
        ]
        for name in non_replicable:
            self.assertNotIn(name, schemas)

    def test_schemas_and_metadata_keys_match(self):
        """Every entry in schemas has a corresponding field_metadata entry."""
        schemas, field_metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(field_metadata.keys()))

    def test_schemas_are_dicts(self):
        """Each schema value is a dict."""
        schemas, _ = get_schemas()
        for name, schema in schemas.items():
            self.assertIsInstance(
                schema, dict, msg="{} schema is not a dict".format(name)
            )

    def test_field_metadata_are_lists(self):
        """Each field_metadata value is a list (singer metadata format)."""
        _, field_metadata = get_schemas()
        for name, mdata in field_metadata.items():
            self.assertIsInstance(
                mdata, list,
                msg="{} metadata is not a list".format(name)
            )

    def test_key_properties_in_metadata(self):
        """Each stream's root metadata contains table-key-properties."""
        _, field_metadata = get_schemas()
        for name, mdata in field_metadata.items():
            root_meta = next(
                (m.get('metadata', {})
                 for m in mdata if m.get('breadcrumb') == ()),
                {}
            )
            self.assertIn(
                'table-key-properties', root_meta,
                msg="{} missing table-key-properties".format(name)
            )

    def test_replication_method_in_metadata(self):
        """Each stream's root metadata contains forced-replication-method."""
        _, field_metadata = get_schemas()
        for name, mdata in field_metadata.items():
            root_meta = next(
                (m.get('metadata', {})
                 for m in mdata if m.get('breadcrumb') == ()),
                {}
            )
            self.assertIn(
                'forced-replication-method', root_meta,
                msg="{} missing forced-replication-method".format(name)
            )

    def test_no_client_required(self):
        """get_schemas() must succeed without any client argument."""
        try:
            get_schemas()
        except TypeError as exc:  # pragma: no cover
            self.fail(
                "get_schemas() raised TypeError (unexpected arg): {}".format(exc)
            )


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
