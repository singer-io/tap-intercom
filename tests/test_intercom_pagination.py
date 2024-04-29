"""
Test tap pagination of streams
"""

from math import ceil
from tap_tester import runner, connections

from base import IntercomBaseTest

class RechargePaginationTest(IntercomBaseTest):
    
    @staticmethod
    def name():
        return "tap_tester_intercom_pagination_test"

    def get_properties(self):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date' : "2016-01-01T00:00:00Z"
        }
        return return_value

    def test_run(self):
        """
            Verify that for each stream you can get multiple pages of data
            and that when all fields are selected more than the automatic fields are replicated.
            PREREQUISITE
            For EACH stream add enough data that you surpass the limit of a single
            fetch of data.  For instance if you have a limit of 100 records ensure
            that 101 (or more) records have been posted for that stream.
        """
        page_size = 50
        conn_id = connections.ensure_connection(self)

        # Checking pagination for streams having enough data
        expected_streams = [
            # "conversations",
            "contacts",
            # "tags",
            "companies"
        ]
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Expected values
                expected_primary_keys = self.expected_primary_keys()

                # Collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync = record_count_by_stream.get(stream, 0)
                primary_keys_list = [tuple(message.get('data').get(expected_pk)
                                           for expected_pk in expected_primary_keys[stream])
                                     for message in synced_records.get(stream).get('messages')
                                     if message.get('action') == 'upsert']

                # Verify records are more than page size so multiple pages is working
                self.assertGreater(record_count_sync, page_size)

                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / page_size)
                for page_index in range(page_count):
                    page_start = page_index * page_size
                    page_end = (page_index + 1) * page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            # don't compare the page to itself
                            if current_index == other_index:
                                continue

                            self.assertTrue(current_page.isdisjoint(other_page), msg=f'other_page_primary_keys={other_page}')
