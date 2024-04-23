import datetime
import dateutil.parser

from tap_tester import runner, menagerie, connections

from base import IntercomBaseTest


class IntercomBookmarks(IntercomBaseTest):
    @staticmethod
    def name():
        return "tap_tester_intercom_bookmarks"

    def calculated_states_by_stream(self, current_state):
        """
            Look at the bookmarks from a previous sync and set a new bookmark
            value based on timedelta expectations. This ensures the subsequent sync will replicate
            at least 1 record but, fewer records than the previous sync.

            If the test data is changed in the future this will break expectations for this test.

            The following streams barely cut:

            companies           "2021-06-14T00:00:00.000000Z"
                                "2021-06-15T00:00:00.000000Z"
            company_segments    "2021-06-14T00:00:00.000000Z"
                                "2021-06-15T00:00:00.000000Z"
            conversations       '2021-06-14T00:00:00.000000Z'
                                '2021-06-15T00:00:00.000000Z'
            contacts            '2021-06-14T00:00:00.000000Z'
                                '2021-06-15T00:00:00.000000Z'
            segments            '2021-06-14T00:00:00.000000Z'
                                '2021-06-15T00:00:00.000000Z'
        """
        timedelta_by_stream = {stream: [1,0,0]  # {stream_name: [days, hours, minutes], ...}
                               for stream in self.expected_streams()}

        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}
        for stream, state in current_state['bookmarks'].items():
            state_key, state_value = list(state.keys())[0], list(state.values())[0]
            state_as_datetime = dateutil.parser.parse(state_value)

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - datetime.timedelta(days=days, hours=hours, minutes=minutes)

            state_format = '%Y-%m-%dT%H:%M:%S-00:00'
            calculated_state_formatted = datetime.datetime.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state[stream] = {state_key: calculated_state_formatted}

        return stream_to_calculated_state

    def test_run(self):
        """
            Verify that:
                For each stream, you can do a sync that records bookmarks.
                The bookmark is the maximum value sent to the target for the replication key.
                A second sync respects the bookmark
                All data of the second sync is >= the bookmark from the first sync
                The number of records in the 2nd sync is less than the first (This assumes that
                        new data added to the stream is done at a rate slow enough that you haven't
                        doubled the amount of data from the start date to the first sync between
                        the first sync and second sync run in this test)
                For the full table stream, all data replicated in sync 1 is replicated again in sync 2.

            PREREQUISITE
            For EACH stream that is incrementally replicated, there are multiple rows of data with
                different values for the replication key
        """

        # Streams for which we cannot generate data
        # The stream: "conversation_parts" is child stream and bookmark is being written of parent stream. Thus, skipping the stream
        untestable_streams = {"companies", "segments", "company_segments", "conversation_parts", "tags", "conversations"}
        # Contacts stream does 3 API calls for addressable list fields, [notes, companies, tags]
        # This cause the build to run more than 3 hrs, thus skipping this stream
        streams_to_skip = {"contacts"}
        expected_streams =  self.expected_streams() - untestable_streams - streams_to_skip
    
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self, original_properties=True)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)
        first_sync_bookmarks["bookmarks"] = first_sync_bookmarks.get("bookmarks", {})

        ##########################################################################
        ### Update State Between Syncs
        ##########################################################################

        new_states = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        for stream, new_state in simulated_states.items():
            new_states['bookmarks'][stream] = new_state
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        ### Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Test By Stream
        ##########################################################################

        start_date = self.get_properties().get('start_date')
        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(stream, {}).get('messages',{})
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(stream, {}).get('messages',{})
                                        if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)


                if expected_replication_method == self.INCREMENTAL:


                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = list(expected_replication_keys[stream])[0]
                    first_bookmark_value = first_bookmark_key_value.get(replication_key)
                    second_bookmark_value = second_bookmark_key_value.get(replication_key)
                    first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                    second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)
                    simulated_bookmark_value = self.convert_state_to_utc(new_states['bookmarks'][stream][replication_key])

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_key_value.get(replication_key))

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_key_value.get(replication_key))

                    # Verify the second sync bookmark is Greater or Equal to the first sync bookmark
                    self.assertGreaterEqual(second_bookmark_value, first_bookmark_value)


                    for record in first_sync_messages:
                        # Verify the First sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)
                        self.assertLessEqual(
                            replication_key_value, first_bookmark_value_utc,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                        # Verify for the First sync, we got records from the start date
                        self.assertGreaterEqual(
                            replication_key_value, start_date,
                            msg="Second sync records do not respect the previous bookmark.")

                    for record in second_sync_messages:

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value, second_bookmark_value_utc,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                        # Verify the second sync replication key value is Greater or Equal to the set bookmark
                        self.assertGreaterEqual(
                            replication_key_value, simulated_bookmark_value,
                            msg="Second sync records do not respect the previous bookmark.")

                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                else:

                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream, expected_replication_method)
                    )


                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
