from tap_tester import connections, runner

from base import IntercomBaseTest


class IntercomStartDateTest(IntercomBaseTest):

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_intercom_start_date_test"

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        # This test was failing for `segments` stream, as there was no data to be found
        # for currently configured start date. So added it to untestable_streams.
        # Start date is configured to current value in base.py so that integration tests
        # should finish quickly and don't run for hours
        untestable_streams = {"segments"}
        expected_streams =  self.expected_streams().difference(untestable_streams)

        self.start_date_1 = self.get_properties().get('start_date')
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=3)

        self.start_date = self.start_date_1

        ##########################################################################
        ### First Sync
        ##########################################################################

        # instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        # run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]

                # collect information for assertions from syncs 1 & 2 base on expected Primary key values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)
                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert']
                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                # collect information for assertions from syncs 1 & 2 base on expected Replication key values
                replication_keys_list_1 = [tuple(message.get('data').get(expected_rk) for expected_rk in expected_replication_keys)
                                       for message in synced_records_1.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert']
                replication_keys_list_2 = [tuple(message.get('data').get(expected_rk) for expected_rk in expected_replication_keys)
                                       for message in synced_records_2.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert']
                replication_keys_sync_1 = set(replication_keys_list_1)
                replication_keys_sync_2 = set(replication_keys_list_2)


                # Verify that the 2nd sync with a later start date replicates the same or less number of
                # records as the 1st sync.
                self.assertGreaterEqual(record_count_sync_1, record_count_sync_2)

                # Verify by primary key that atleast some common records are replicated in the 1st and 2nd syncs
                self.assertNotEqual(primary_keys_sync_1.intersection(primary_keys_sync_2), set())

                # Verify by replication key that atleast some common records are replicated in the 1st and 2nd syncs
                self.assertNotEqual(replication_keys_sync_1.intersection(replication_keys_sync_2), set())
