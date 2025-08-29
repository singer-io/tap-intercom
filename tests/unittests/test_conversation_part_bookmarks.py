import unittest
import singer
from unittest import mock
from tap_intercom.streams import Conversations
from tap_intercom.client import IntercomClient

class Schema:
    def __init__(self, stream):
        self.stream = stream

    def to_dict(self):
        return {'stream': self.stream}

class Stream:
    def __init__(self, stream_name):
        self.tap_stream_id = stream_name
        self.stream = stream_name
        self.replication_key = 'updated_at'
        self.schema = Schema(stream_name)
        self.metadata = {}

class Catalog:
    def __init__(self, streams):
        self.streams = streams

    def get_selected_streams(self, state):
        for stream in self.streams:
            yield Stream(stream)

    def get_stream(self, stream_name):
        return Stream(stream_name)

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
            {"id": 1, "updated_at": 1640636000000}, # UTC datetime "2021-12-27T20:13:20.000000Z" for 1640636000
            {"id": 2, "updated_at": 1640637000000}  # UTC datetime "2021-12-27T20:30:00.000000Z" for 1640637000
        ]
        # Mocked child records
        mocked_client_get.return_value = {}

        # Initialize IntercomClient and Conversations object
        client = IntercomClient('dummy_token', None)
        conversation_part = Conversations(client=client, catalog=Catalog(['conversations', 'conversation_parts']), selected_streams=['conversations', 'conversation_parts'])

        tap_state = {}
        # Call get_records() of conversation_parts which writes bookmark
        list(conversation_part.sync(tap_state, {}, {}, {'start_date': '2021-12-25T00:00:00Z'}, None))

        # Expected call of write_bookmark() function
        state = {'bookmarks': {'conversation_parts': {'updated_at': '2021-12-27T20:30:00.000000Z'}, 'conversations': {'updated_at': '2021-12-27T20:30:00.000000Z'}}}
        expected_write_bookmark = [
            # Bookmark update after first parent(2021-12-27T20:13:20.000000Z)
            mock.call(state, 'conversation_parts', 'updated_at', '2021-12-27T20:13:20.000000Z'),
            # Bookmark update after second parent(2021-12-27T20:30:00.000000Z)
            mock.call(state, 'conversation_parts', 'updated_at', '2021-12-27T20:30:00.000000Z'),
            # Parent stream bookmark
            mock.call(state, 'conversations', 'updated_at', '2021-12-27T20:30:00.000000Z')
        ]

        # Verify that write_bookmark() is called with expected values
        self.assertEqual(mocked_write_bookmark.mock_calls, expected_write_bookmark)
        # Verify we get 'conversation_parts' bookmark
        self.assertIsNotNone(tap_state.get('bookmarks').get('conversation_parts'))
