from datetime import datetime as dt
from tap_tester import runner, connections, menagerie
from base import IntercomBaseTest

class intercomInterruptedSyncTest(IntercomBaseTest):

    def assertIsDateFormat(self, value, str_format):
        """
            Assertion method that verifies a string value is a formatted datetime with
            the specified format.
        """
        try:
            dt.strptime(value, str_format)
        except ValueError as err:
            raise AssertionError(f"Value: {value} does not conform to expected format: {str_format}") from err


    def name(self):
        """Returns the name of the test case"""

        return "intercom_interrupted_sync_test"

    
    def test_run(self):
        """
            Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                    The next sync job kicks off and the tap picks back up on that `currently_syncing` stream.
            Expected State Structure:
                {
                    "currently_syncing": "stream-name",
                    "bookmarks": {
                        "stream-name-1": "bookmark-date"
                        "stream-name-2": "bookmark-date"
                    }
                }
            Test Cases:
            - Verify an interrupted sync can resume based on the `currently_syncing` and stream level bookmark value.
            - Verify only records with replication-key values greater than or equal to the stream level bookmark are
                replicated on the resuming sync for the interrupted stream.
            - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync.
        """

        self.start_date = "2022-07-05T00:00:00Z"
        start_date_datetime = dt.strptime(self.start_date, "%Y-%m-%dT%H:%M:%SZ")

        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self, original_properties=False)

        expected_streams = {"company_segments", "conversations", "segments", "admins", "contacts", "companies"}

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        for catalog in found_catalogs:
            if catalog["stream_name"] not in expected_streams:
                continue

            annoted_schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            # De-select all fields
            non_selected_properties = annoted_schema.get("annotated-schema", {}).get("properties", {})
            non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, annoted_schema, \
                additional_md=additional_md, non_selected_fields=non_selected_properties)
            
        # Run sync
        record_count_by_stream_full_sync = self.run_and_verify_sync(conn_id)
        synced_records_full_sync = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update the state between the syncs
        ##########################################################################

        # State for 2nd sync
        state = {
            "currently_syncing": "conversations",
            "bookmarks": {
                "conversations": {
                    "updated_at": "2022-07-07T15:15:47.000000Z"
                },
                "company_segments": {
                    "updated_at": "2022-07-19T15:30:19.000000Z"
                }
            }
        }
            
        # Set state for 2nd sync
        menagerie.set_state(conn_id, state)

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Run sync after interruption
        record_count_by_stream_interrupted_sync = self.run_and_verify_sync(conn_id)
        synced_records_interrupted_sync = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get('currently_syncing')

        expected_replication_method = self.expected_replication_method()

        # Checking resuming sync resulted in a successfully saved state
        for stream in expected_streams:
            with self.subTest(stream=stream ):
                # Verify sync is not interrupted by checking currently_syncing in the state for sync
                self.assertIsNone(currently_syncing)

                # Verify bookmarks are saved
                self.assertIsNotNone(final_state.get('bookmarks'))

                if expected_replication_method[stream] == self.INCREMENTAL:
                    expected_replication_key = list(self.expected_replication_keys()[stream])[0]

                    # Verify final_state is greater than or equal to uninterrupted sync's state.
                    # (This is what the value would have been without an interruption and proves resuming succeeds)
                    # As live data is received and bookmark is getting updated in between the sync, therefore asserting greater than or equal.
                    for stream_name, bookmark_final_state in final_state.get('bookmarks').items():
                        self.assertGreaterEqual(bookmark_final_state.get(expected_replication_key), \
                            full_sync_state.get('bookmarks')[stream_name].get(expected_replication_key))
                else:
                    # Verify we do not store any state for FULL_TABLE streams
                    self.assertIsNone(full_sync_state.get('bookmarks').get(stream))
                    self.assertIsNone(final_state.get('bookmarks').get(stream))

        # Stream level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):
                
                # Gather Actual results
                full_records = [message['data'] for message in synced_records_full_sync.get(stream, {}).get('messages', [])]
                full_record_count = record_count_by_stream_full_sync.get(stream, 0)
                interrupted_records = [message['data'] for message in synced_records_interrupted_sync.get(stream, {}).get('messages', [])]
                interrupted_record_count = record_count_by_stream_interrupted_sync.get(stream, 0)

                if expected_replication_method == self.INCREMENTAL:

                    # Gather Expectations
                    expected_replication_key = list(self.expected_replication_keys()[stream])[0]
                    # Final bookmark after interrupted sync
                    final_stream_bookmark = final_state['bookmarks'][stream].get("updated_at")
                    final_state_bookmark_datetime = dt.strptime(final_stream_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")
                    full_sync_bookmark = full_sync_state['bookmarks'][stream].get("updated_at")
                    full_sync_bookmark_datetime = dt.strptime(full_sync_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")
                    
                    # Verify final bookmark saved matches formatting standards for resuming sync
                    self.assertIsNotNone(final_stream_bookmark)
                    self.assertIsInstance(final_stream_bookmark, str)
                    self.assertIsDateFormat(str(final_stream_bookmark), "%Y-%m-%dT%H:%M:%S.%fZ")


                    if stream == state['currently_syncing']:
                       
                        # Check if the interrupted stream has a bookmark written for it
                        interrupted_stream_bookmark = state['bookmarks'][stream].get("updated_at")
                        interrupted_bookmark_datetime = dt.strptime(interrupted_stream_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")

                        # - Verify resuming sync only replicates records with replication key values greater or equal to
                        #       the state for streams that were replicated during the interrupted sync.
                        # - Verify the interrupted sync replicates the expected record set
                        for record in interrupted_records:
                            rec_time = dt.strptime(record.get(expected_replication_key), "%Y-%m-%dT%H:%M:%S.%fZ")
                            self.assertGreaterEqual(rec_time, interrupted_bookmark_datetime)

                            # As live data is received and bookmark is getting updated in between the sync, some records might not be present.
                            if full_sync_bookmark_datetime > final_state_bookmark_datetime: 
                                self.assertIn(record, full_records, msg='Incremental table record in interrupted sync not found in full sync')

                        # Record count for all streams of interrupted sync match expectations
                        full_records_after_interrupted_bookmark = 0
                        for record in full_records:
                            rec_time = dt.strptime(record.get(expected_replication_key), "%Y-%m-%dT%H:%M:%S.%fZ")
                            if rec_time >= interrupted_bookmark_datetime:
                                full_records_after_interrupted_bookmark += 1

                        self.assertEqual(full_records_after_interrupted_bookmark, interrupted_record_count, \
                                                msg='Expected {} records in each sync'.format(full_records_after_interrupted_bookmark))

                    else:
                        # Get the date to start 2nd sync for non-interrupted streams
                        synced_stream_bookmark = state['bookmarks'].get(stream, {}).get("updated_at", None)
                        if synced_stream_bookmark:
                            synced_stream_datetime = dt.strptime(synced_stream_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")
                        else:
                            synced_stream_datetime = start_date_datetime
                        
                        # Verify we replicated some records for the non-interrupted streams
                        self.assertGreater(interrupted_record_count, 0)
                        
                        # - Verify resuming sync only replicates records with replication key values greater or equal to
                        #       the state for streams that were replicated during the interrupted sync.
                        # - Verify resuming sync replicates all records that were found in the full sync (un-interupted)
                        for record in interrupted_records:
                            rec_time = dt.strptime(record.get(expected_replication_key), "%Y-%m-%dT%H:%M:%S.%fZ")
                            self.assertGreaterEqual(rec_time, synced_stream_datetime)

                            self.assertIn(record, full_records, msg='Unexpected record replicated in resuming sync.')
                        
                        # Verify we replicated all the records from 1st sync for the streams
                        #       that are left to sync (ie. streams without bookmark in the state)
                        if stream not in state["bookmarks"].keys(): 
                            for record in full_records:
                                self.assertIn(record, interrupted_records, msg='Record missing from resuming sync.' )



                elif expected_replication_method == self.FULL_TABLE:

                    # Verify full table streams do not save bookmarked values at the conclusion of a successful sync
                    self.assertNotIn(stream, full_sync_state['bookmarks'].keys())
                    self.assertNotIn(stream, final_state['bookmarks'].keys())

                    # Verify first and the second sync have the same records
                    self.assertEqual(full_record_count, interrupted_record_count)
                    for rec in interrupted_records:
                        self.assertIn(rec, full_records, msg='full table record in interrupted sync not found in full sync')
