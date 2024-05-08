"""
Test that with no fields selected for a stream automatic fields are still replicated
"""
from tap_tester import runner, connections, menagerie

from base import IntercomBaseTest


class IntercomAllFields(IntercomBaseTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    # Fields for which we cannot generate data
    fields_to_remove = {
        'company_attributes': {
            'options',
            'admin_id'
        },
        'companies': {
            'size',
            'website',
            'industry',
            'segments',
            'tags'
        },
        'conversations': {
            'user',
            'customer_first_reply',
            'sent_at',
            'assignee',
            'customers',
            'conversation_message'
        },
        'admins': {
            'admin_ids'
        },
        'contact_attributes': {
            'options',
            'admin_id'
        },
        'contacts': {
            'tags'
        }
    }

    def get_properties(self):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date' : "2016-01-01T00:00:00Z"
        }
        return return_value

    @staticmethod
    def name():
        return "tap_tester_intercom_all_fields"

    def test_run(self):
        """
            • Verify no unexpected streams were replicated
            • Verify that more than just the automatic fields are replicated for each stream. 
            • verify all fields for each stream are replicated
        """
        # Created card for untestable/unstable streams.
        # FIX CARD: https://jira.talendforge.org/browse/TDL-17035
        untestable_streams = {"tags", "segments" ,"conversation_parts", "conversations", "company_segments"}
        expected_streams =  self.expected_streams().difference(untestable_streams)

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        our_catalogs = [catalog for catalog in found_catalogs if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, our_catalogs, select_all_fields=True)

        # Grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in our_catalogs:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(
                fields_from_field_level_md)

        # Run sync mode
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)


        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]
                expected_automatic_keys = self.expected_automatic_fields().get(
                    stream, set())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # Verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))   

                messages = synced_records.get(stream)
                # Collect actual values
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                expected_all_keys = expected_all_keys - self.fields_to_remove.get(stream,set())
                self.assertSetEqual(expected_all_keys, actual_all_keys)
