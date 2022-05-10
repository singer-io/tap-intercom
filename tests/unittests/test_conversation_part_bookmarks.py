import unittest
import singer
from unittest import mock
from tap_intercom.streams import ConversationParts
from tap_intercom.client import IntercomClient

class TestConversationPartsBookmarking(unittest.TestCase):

    @mock.patch("tap_intercom.streams.singer.write_bookmark", side_effect=singer.write_bookmark)
    @mock.patch("tap_intercom.streams.Conversations.get_records")
    @mock.patch("tap_intercom.client.IntercomClient.get")
    def test_conversation_parts_bookmarking(self, mocked_client_get, mocked_parent_records, mocked_write_bookmark):
        '''
            Verify that state is updated with parent's updated_at after syncing conversation_parts for each coversation.
        '''
        # Mocked parent records
        mocked_parent_records.return_value = [
            {"id": 1, "updated_at": 1640636000}, # UTC datetime "2021-12-27T20:13:20.000000Z" for 1640636000
            {"id": 2, "updated_at": 1640637000}  # UTC datetime "2021-12-27T20:30:00.000000Z" for 1640637000
        ]
        # Mocked child records
        mocked_client_get.return_value = {}

        # Initialize IntercomClient and ConversationParts object
        client = IntercomClient('dummy_token', None)
        conversation_part = ConversationParts(client)

        # Call get_records() of conversation_parts which writes bookmark
        records = list(conversation_part.get_records("test", False, {}))

        # Expected call of write_bookmark() function
        state = {'bookmarks': {'conversation_parts': {'updated_at': '2021-12-27T20:30:00.000000Z'}}}
        expected_write_bookmark = [
            # Bookmark update after first parent(2021-12-27T20:13:20.000000Z)
            mock.call(state, 'conversation_parts', 'updated_at', '2021-12-27T20:13:20.000000Z'),
            # Bookmark update after second parent(2021-12-27T20:30:00.000000Z)
            mock.call(state, 'conversation_parts', 'updated_at', '2021-12-27T20:30:00.000000Z')
        ]

        # Verify that write_bookmark() is called with expected values
        self.assertEquals(mocked_write_bookmark.mock_calls, expected_write_bookmark)
