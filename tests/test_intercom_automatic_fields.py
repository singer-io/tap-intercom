"""
Test that with no fields selected for a stream automatic fields are still replicated
"""
from tap_tester import runner, connections

from base import IntercomBaseTest


class IntercomAutomaticFields(IntercomBaseTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_intercom_automatic_fields"

    def get_properties(self):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date' : "2016-01-01T00:00:00Z"
        }
        return return_value

    def test_run(self):
        """
            Verify that for each stream you can get multiple pages of data
            when no fields are selected and only the automatic fields are replicated.

            PREREQUISITE
            For EACH stream add enough data that you surpass the limit of a single
            fetch of data.  For instance, if you have a limit of 250 records ensure
            that 251 (or more) records have been posted for that stream.
        """
        # Created card for untestable/unstable streams.
        # FIX CARD: https://jira.talendforge.org/browse/TDL-17035
        untestable_streams = {"conversation_parts", "conversations", "segments", "tags", "company_segments"}
        expected_streams =  self.expected_streams().difference(untestable_streams)

        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False,
        )

        # Run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_keys = self.expected_automatic_fields().get(stream)
                expected_primary_keys = self.expected_primary_keys()[stream]

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row.get('data').keys()) for row in data.get('messages', {})
                                        if row['action'] == 'upsert']

                for message in data.get("messages"):
                    if message.get("action") == "upsert":
                        primary_keys_list = [
                            tuple(
                                message.get("data").get(expected_pk)
                                for expected_pk in expected_primary_keys
                            )
                        ]

                unique_primary_keys_list = set(primary_keys_list)

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit for the {} stream".format(stream))

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # Verify that all replicated records have unique primary key values.
                self.assertCountEqual(
                    primary_keys_list,
                    unique_primary_keys_list,
                    msg="Replicated record does not have unique primary key values.",
                )
