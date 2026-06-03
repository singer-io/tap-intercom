import unittest
from unittest import mock
from tap_intercom.client import IntercomClient, IntercomNotFoundError
from tap_intercom.streams import Contacts
from test_conversation_part_bookmarks import Catalog


PARENT_RECORD = {
    'id': 'contact_1',
    'email': 'joe@example.com',
    'name': 'Joe',
    'role': 'user',
    'updated_at': 1640636000000,
    'created_at': 1640636000000,
}

CONTACT_DETAILS_RESPONSE = dict(PARENT_RECORD, **{
    'opted_in_subscription_types': {'type': 'list', 'data': [], 'url': '/contacts/contact_1/subscriptions', 'total_count': 0, 'has_more': False},
    'utm_source': 'google',
})


def make_stream(selected_streams):
    client = IntercomClient('dummy_token', None)
    return Contacts(client=client, catalog=Catalog(selected_streams), selected_streams=selected_streams)


@mock.patch('tap_intercom.streams.singer.write_schema')
@mock.patch('tap_intercom.streams.Contacts.get_records', return_value=[PARENT_RECORD])
@mock.patch('tap_intercom.streams.singer.write_bookmark', side_effect=lambda state, *a, **kw: state)
@mock.patch('tap_intercom.streams.singer.write_record')
@mock.patch('tap_intercom.streams.singer.write_state')
class TestContactDetailsSync(unittest.TestCase):
    """Tests for the ContactDetails child stream."""

    @mock.patch('tap_intercom.streams.IntercomClient.get', return_value=CONTACT_DETAILS_RESPONSE)
    def test_contact_details_record_written(
        self, mock_client_get, mock_write_state, mock_write_record,
        mock_write_bookmark, mock_get_records, mock_write_schema
    ):
        """When GET /contacts/{id} succeeds, a full contact_details record is written."""
        contacts = make_stream(['contacts', 'contact_details'])
        contacts.sync(state={}, stream_schema={}, stream_metadata={}, config={'start_date': '2021-01-01'}, transformer=None)

        mock_client_get.assert_called_once_with('contacts/contact_1', params={})
        mock_write_record.assert_called()
        stream, record = mock_write_record.call_args[0]
        self.assertEqual(stream, 'contact_details')
        self.assertEqual(record.get('id'), 'contact_1')

    @mock.patch('tap_intercom.streams.IntercomClient.get',
                side_effect=IntercomNotFoundError('HTTP-error-code: 404, Error:User Not Found, Error_Code:not_found'))
    def test_user_not_found_writes_partial_record(
        self, mock_client_get, mock_write_state, mock_write_record,
        mock_write_bookmark, mock_get_records, mock_write_schema
    ):
        """When GET /contacts/{id} returns 404 User Not Found, a partial record from the parent is written."""
        contacts = make_stream(['contacts', 'contact_details'])
        contacts.sync(state={}, stream_schema={}, stream_metadata={}, config={'start_date': '2021-01-01'}, transformer=None)

        mock_write_record.assert_called()
        stream, record = mock_write_record.call_args[0]
        self.assertEqual(stream, 'contact_details')
        self.assertEqual(record.get('id'), 'contact_1')
        self.assertEqual(record.get('email'), 'joe@example.com')

    @mock.patch('tap_intercom.streams.IntercomClient.get',
                side_effect=IntercomNotFoundError('HTTP-error-code: 404, Error:Resource Not Found, Error_Code:not_found'))
    def test_other_404_raises(
        self, mock_client_get, mock_write_state, mock_write_record,
        mock_write_bookmark, mock_get_records, mock_write_schema
    ):
        """When GET /contacts/{id} returns a non-User-Not-Found 404, the exception is re-raised."""
        contacts = make_stream(['contacts', 'contact_details'])
        with self.assertRaises(IntercomNotFoundError):
            contacts.sync(state={}, stream_schema={}, stream_metadata={}, config={'start_date': '2021-01-01'}, transformer=None)

    @mock.patch('tap_intercom.streams.IntercomClient.get', return_value=CONTACT_DETAILS_RESPONSE)
    def test_contact_details_not_synced_when_not_selected(
        self, mock_client_get, mock_write_state, mock_write_record,
        mock_write_bookmark, mock_get_records, mock_write_schema
    ):
        """When contact_details is not selected, no GET /contacts/{id} calls are made."""
        contacts = make_stream(['contacts'])
        contacts.sync(state={}, stream_schema={}, stream_metadata={}, config={'start_date': '2021-01-01'}, transformer=None)

        mock_client_get.assert_not_called()
