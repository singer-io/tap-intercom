import unittest
from unittest import mock
from parameterized import parameterized
from tap_intercom.client import IntercomClient
from tap_intercom.streams import Admins, BaseStream, CompanyAttributes, ContactAttributes

def transform(*args, **kwargs):
    return args[0]

class TestSDCRecordHash(unittest.TestCase):
    @parameterized.expand([
        ['default_attr', {'name': 'test 1', 'description': 'test 1 description', 'label': 'test1'}],
        ['default_attr_without_description', {'name': 'test 1', 'label': 'test1'}],
        ['custom_attr', {'id': 11, 'name': 'test 1', 'description': 'test 1 description', 'label': 'test1'}],
        ['custom_attr_without_description', {'id': 11, 'name': 'test 1', 'label': 'test1'}],
    ])
    def test_generate_hash(self, name, test_data):
        client = IntercomClient('test_access_token', 300)
        stream = BaseStream(client)
        hashed_records = stream.generate_record_hash(test_data)

        self.assertIsNotNone(hashed_records)
        self.assertIsNotNone(hashed_records.get('_sdc_record_hash'))

    @parameterized.expand([
        ['company_attr', CompanyAttributes, True],
        ['contact_attr', ContactAttributes, True],
        ['admins', Admins, False],
    ])
    @mock.patch('singer.write_record')
    @mock.patch('tap_intercom.streams.transform', side_effect = transform)
    @mock.patch('tap_intercom.streams.CompanyAttributes.get_records', side_effect = [[{'id': 1, 'name': 'test1'}, {'id': 2, 'name': 'test2'}]])
    @mock.patch('tap_intercom.streams.ContactAttributes.get_records', side_effect = [[{'id': 1, 'name': 'test1'}, {'id': 2, 'name': 'test2'}]])
    @mock.patch('tap_intercom.streams.Admins.get_records', side_effect = [[{'id': 1, 'name': 'test1'}, {'id': 2, 'name': 'test2'}]])
    def test_sync_hashed(self, name, test_data, expected_data, mocked_admins_get_records, mocked_contacts_attr_get_records, mocked_company_attr_get_records, mocked_transform, mocked_write_record):
        client = IntercomClient('test_access_token', 300)
        stream = test_data(client)
        stream.sync({}, {}, {}, {}, None)

        write_record_calls = mocked_write_record.mock_calls

        self.assertIsNotNone(write_record_calls)
        for call in write_record_calls:
            self.assertEqual(call[1][1].get('_sdc_record_hash') is not None, expected_data)
