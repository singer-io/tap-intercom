import os
from functools import reduce

from tap_tester import connections, menagerie, runner

from base import IntercomBaseTest


class IntercomFieldSelection(IntercomBaseTest):  # TODO use base.py, determine if test is needed

    @staticmethod
    def name():
        return "tap_tester_intercom_field_selection"

    @staticmethod
    def expected_check_streams():
        return {
            'admins',
            'companies',
            'company_attributes',
            'company_segments',
            'conversations',
            'conversation_parts',
            'contact_attributes',
            'contacts',
            'segments',
            'tags',
            'teams',
        }

    @staticmethod
    def expected_sync_streams():
        return {
            'admins',
            'companies',
            'company_attributes',
            'company_segments',
            'conversations',
            'conversation_parts',
            'contact_attributes',
            'contacts',
            'segments',
            'tags',
            'teams',
        }

    @staticmethod
    def expected_pks():
        return {
            "admins" :              {"id"},
            "companies" :           {"id"},
            "company_attributes" :  {"id"},
            "company_segments" :    {"id"},
            "conversations" :       {"id"},
            "conversation_parts" :  {"id"},
            "contact_attributes" :  {"id"},
            "contacts":             {"id"},
            "segments" :            {"id"},
            "tags" :                {"id"},
            "teams" :               {"id"},
        }

    def expected_automatic_fields(self):
        return self.expected_pks()

    def get_properties(self):  # pylint: disable=arguments-differ
        return {'start_date' : '2015-03-15T00:00:00Z',
            'access_token': os.getenv('TAP_INTERCOM_ACCESS_TOKEN'),
            'start_date' : '2019-01-01T00:00:00Z',
            'user_agent': 'tap-intercom <api_user_email@your_company.com>'
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are kosher")

        all_excluded_fields = {}
        # select all catalogs
        for c in found_catalogs:
            if c['stream_name'] == 'ads':
                continue

            discovered_schema = menagerie.get_annotated_schema(conn_id, c['stream_id'])['annotated-schema']
            all_excluded_fields[c['stream_name']] = list(set(discovered_schema.keys()) - self.expected_automatic_fields().get(c['stream_name'], set()))[:5]
            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                c,
                discovered_schema,
                non_selected_fields=all_excluded_fields[c['stream_name']])

        # clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # This should be validating the the PKs are written in each record
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        synced_records = runner.get_records_from_target_output()
        self.assertTrue('ads' not in synced_records.keys())
        for stream_name, data in synced_records.items():
            record_messages = [set(row['data'].keys()) for row in data['messages']]
            for record_keys in record_messages:
                # The intersection should be empty
                self.assertFalse(record_keys.intersection(all_excluded_fields[stream_name]))