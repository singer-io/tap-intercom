import unittest
from unittest import mock
from parameterized import parameterized
from tap_intercom.client import IntercomClient
from tap_intercom.streams import Conversations
from tap_intercom.sync import sync
import singer
from test_conversation_part_bookmarks import Catalog

# Mock function for transform
def transform(*args, **kwargs):
    # Return record itself
    return args[0]

class TestParentChildSync(unittest.TestCase):
    """
        Test cases to verify the parent-child behaviour
    """

    @parameterized.expand([
        ['parent_child_selected', ['conversations', 'conversation_parts'], {'stream': 'conversations'}],
        ['parent_selected', ['conversations'], {'stream': 'conversations'}],
        ['child_selected', ['conversation_parts'], {'stream': 'conversations'}]
    ])
    @mock.patch('tap_intercom.streams.IncrementalStream.sync', return_value = {})
    def test_parent_child_selection(self, name, test_data, expected_data, mocked_sync):
        """
            Test case to verify we add parent stream to sync if
            child is selected and parent stream is not selected
        """
        sync({}, {}, Catalog(test_data))
        args, kwargs = mocked_sync.call_args
        # verify the 'sync' is called with parent stream
        self.assertEqual(args[1], expected_data)

    @parameterized.expand([
        ['parent_child_selected_start_date', [['conversations', 'conversation_parts'], {}], '2021-01-01'],
        ['parent_child_selected_state', [['conversations', 'conversation_parts'], {'bookmarks': {'conversation_parts': {'updated_at': '2021-01-02'}, \
            'conversations': {'updated_at': '2021-01-03'}}}], '2021-01-02'],
        ['parent_selected_start_date', [['conversations'], {}], '2021-01-01'],
        ['parent_selected_state', [['conversations'], {'bookmarks': {'conversation_parts': {'updated_at': '2021-01-02'}, \
            'conversations': {'updated_at': '2021-01-03'}}}], '2021-01-03'],
        ['child_selected_start_date', [['conversation_parts'], {}], '2021-01-01'],
        ['child_selected_state', [['conversation_parts'], {'bookmarks': {'conversation_parts': {'updated_at': '2021-01-02'}, \
            'conversations': {'updated_at': '2021-01-03'}}}], '2021-01-02'],
    ])
    @mock.patch('singer.write_schema')
    @mock.patch('tap_intercom.streams.Conversations.get_records')
    def test_parent_child_records_sync(self, name, test_data, expected_data, mocked_get_records, mocked_write_schema):
        """
            Test case to verify we set expected start date to sync for parent-child streams
        """
        client = IntercomClient('test_access_token', 300)
        conversations = Conversations(client=client, catalog=Catalog(test_data[0]), selected_streams=test_data[0])
        state = test_data[1]
        config = {'start_date': '2021-01-01'}
        conversations.sync(state=state, stream_schema={}, stream_metadata={}, config=config, transformer=None)
        mocked_get_records.assert_called_with(singer.utils.strptime_to_utc(expected_data), metadata={})

@mock.patch('singer.write_schema')
@mock.patch('tap_intercom.streams.Conversations.get_records', side_effect = [[{'id': 1, 'updated_at': 1609804800000}]]) # '2021-01-05'
@mock.patch('tap_intercom.streams.transform', side_effect = transform)
@mock.patch('tap_intercom.streams.BaseStream.sync_substream')
class TestParentChildWriteRecords(unittest.TestCase):
    """
        Test cases to verify the parent-child record writing
    """

    def test_parent_stream_records_writing(self, mocked_sync_substream, mocked_transform, mocked_get_records, mocked_write_schema):
        """
            Test case to verify we write parent records if the stream is selected
        """
        client = IntercomClient('test_access_token', 300)
        conversations = Conversations(client=client, catalog=Catalog(['conversations']), selected_streams=['conversations'])
        config = {'start_date': '2021-01-01'}
        conversations.sync(state={}, stream_schema={}, stream_metadata={}, config=config, transformer=None)
        self.assertEqual(mocked_transform.call_count, 1)

    def test_child_stream_records_writing(self, mocked_sync_substream, mocked_transform, mocked_get_records, mocked_write_schema):
        """
            Test case to verify we write child records if the stream is selected
        """
        client = IntercomClient('test_access_token', 300)
        conversations = Conversations(client=client, catalog=Catalog(['conversation_parts']), selected_streams=['conversation_parts'])
        config = {'start_date': '2021-01-01'}
        conversations.sync(state={}, stream_schema={}, stream_metadata={}, config=config, transformer=None)
        self.assertEqual(mocked_sync_substream.call_count, 1)

    def test_parent_and_child_stream_records_writing(self, mocked_sync_substream, mocked_transform, mocked_get_records, mocked_write_schema):
        """
            Test case to verify we write parent and child records if both streams are selected
        """
        client = IntercomClient('test_access_token', 300)
        conversations = Conversations(client=client, catalog=Catalog(['conversations', 'conversation_parts']), selected_streams=['conversations', 'conversation_parts'])
        config = {'start_date': '2021-01-01'}
        conversations.sync(state={}, stream_schema={}, stream_metadata={}, config=config, transformer=None)
        self.assertEqual(mocked_transform.call_count, 1)
        self.assertEqual(mocked_sync_substream.call_count, 1)
