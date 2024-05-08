from tap_tester import connections, menagerie, runner, LOGGER
from base import IntercomBaseTest
from datetime import datetime as dt


class IntercomStartDateTest(IntercomBaseTest):

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_intercom_start_date_test"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date' : "2016-01-01T00:00:00Z"
        }
        return return_value

    def test_run(self):
        """
            Test that the start_date configuration is respected
            • verify that a sync with a later start date has at least one record synced
            and less records than the 1st sync with a previous start date
            • verify that each stream has less records than the earlier start date sync
            • verify all data from later start data has bookmark values >= start_date
            • verify that the minimum bookmark sent to the target for the later start_date sync
            is greater than or equal to the start date
            • verify by primary key values, that all records in the 1st sync are included in the 2nd sync.
        """
        # Created card for untestable/unstable streams.
        # FIX CARD: https://jira.talendforge.org/browse/TDL-17035
        untestable_streams = {"segments", "company_segments", "conversations", "companies", "conversation_parts"}
        expected_streams =  self.expected_streams().difference(untestable_streams)

        self.start_date_1 = self.get_properties().get('start_date')
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=2)

        start_date_1_epoch = self.dt_to_ts(self.start_date_1, self.START_DATE_FORMAT)
        start_date_2_epoch = self.dt_to_ts(self.start_date_2, self.START_DATE_FORMAT)

        self.start_date = self.start_date_1

        ##########################################################################
        ### First Sync
        ##########################################################################

        # Instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        # Run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # Table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # Run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id_1)

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        LOGGER.info("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # Rrun check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # Table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # Run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_metadata = self.expected_metadata()[stream]
                expected_replication_method = self.expected_replication_method()[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected Primary key values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)
                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert']

                # Some streams are dynamic so we may get additional records in the second sync having greater replication key value
                # than first bookmark, so we need to filter such records before validation
                if expected_replication_method == self.INCREMENTAL:
                    first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(
                        stream).get(list(expected_replication_keys)[0], None)
                    primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert' 
                                       and message.get('data').get(list(expected_replication_keys)[0]) <= first_bookmark_key_value]
                else:
                    primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                        for message in synced_records_2.get(stream, {}).get('messages', {})
                                        if message.get('action') == 'upsert']

                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                if expected_metadata[self.OBEYS_START_DATE]:
                    bookmark_keys_list_1 = [message.get('data').get(list(expected_replication_keys)[0])
                                            for message in synced_records_1.get(stream).get('messages')
                                            if message.get('action') == 'upsert']

                    if expected_replication_method == self.INCREMENTAL:
                        bookmark_keys_list_2 = [message.get('data').get(list(expected_replication_keys)[0])
                                                for message in synced_records_2.get(stream).get('messages')
                                                if message.get('action') == 'upsert'
                                                and message.get('data').get(list(expected_replication_keys)[0]) <= first_bookmark_key_value]
                    else:
                        bookmark_keys_list_2 = [message.get('data').get(list(expected_replication_keys)[0])
                                                for message in synced_records_2.get(stream).get('messages')
                                                if message.get('action') == 'upsert']

                    bookmark_key_sync_1 = set(bookmark_keys_list_1)
                    bookmark_key_sync_2 = set(bookmark_keys_list_2)

                    # Verify bookmark key values are greater than or equal to the start date of sync 1
                    for bookmark_key_value in bookmark_key_sync_1:
                        self.assertGreaterEqual(self.dt_to_ts(bookmark_key_value, self.RECORD_REPLICATION_KEY_FORMAT), start_date_1_epoch)

                    # Verify bookmark key values are greater than or equal to the start date of sync 2
                    for bookmark_key_value in bookmark_key_sync_2:
                        self.assertGreaterEqual(self.dt_to_ts(bookmark_key_value, self.RECORD_REPLICATION_KEY_FORMAT), start_date_2_epoch)

                    # Verify that the 2nd sync with a later start date replicates the same or less numb of
                    # records as the 1st sync.
                    self.assertGreaterEqual(record_count_sync_1, record_count_sync_2)

                    if stream != 'contacts': # skipping contacts as the data are dynamic
                        # Verify by a primary key that all records in the 2nd sync are included in the 1st sync since the 2nd sync has a later start date.
                        self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1),
                                        msg=f'{primary_keys_sync_2 - primary_keys_sync_1} is not in "expected_all_keys"')

                else:
                    # Verify that the 2nd sync with a later start date replicates the same number of
                    # records as the 1st sync.
                    self.assertEqual(record_count_sync_2, record_count_sync_1)

                    # Verify by primary key the same records are replicated in the 1st and 2nd syncs
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
