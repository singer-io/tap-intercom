import unittest
import singer
import datetime as dt
from unittest import mock
from tap_intercom.client import IntercomClient, IntercomError, IntercomNotFoundError
from parameterized import parameterized
from tap_intercom.streams import (AdminList, BaseStream, Admins, Companies, CompanyAttributes,
                                   Contacts, ContactAttributes, CompanySegments, Conversations,
                                   Segments, Tags, Teams, ConversationParts, IncrementalStream)

class TestData(unittest.TestCase):

    base_client = IntercomClient("test","300")
    @parameterized.expand([
        ['Companies',[Companies, IntercomNotFoundError()], []],
        ['CompanyAttributes',[CompanyAttributes, [{'data':['test1'],'pages':{'next':'abc'}},{'data':['test2'],'pages':{'next':''}}]],['test1','test2']],
        ['CompanySegments',[CompanySegments,[{'segments':['test1'],'pages':{'next':'abc'}},{'segments':['test2'],'pages':{'next':''}}]],['test1','test2']],
        ['Segments',[Segments,[{'segments':['test1'],'pages':{'next':'abc'}},{'segments':['test2'],'pages':{'next':''}}]],['test1','test2']],
        ['ContactAttributes',[ContactAttributes,[{'data':['test1'],'pages':{'next':'abc'}},{'data':['test2'],'pages':{'next':''}}]],['test1','test2']],
        ['Tags',[Tags,[{'data':['test1'],'pages':{'next':'abc'}},{'data':['test2'],'pages':{'next':''}}]],['test1','test2']],
        ['Teams',[Teams,[{'teams':['test1'],'pages':{'next':'abc'}},{'teams':['test2'],'pages':{'next':''}}]],['test1','test2']],
        ])

    @mock.patch("tap_intercom.client.IntercomClient.get")
    def test_get_records(self,name, data, expected_data, mocked_client):
        """
        Verify get_records for stream
        """

        test_stream = data[0](self.base_client, None, [])
        mocked_client.side_effect = data[1]

        test_data = list(test_stream.get_records())
        self.assertEqual(test_data,expected_data)

    @mock.patch("tap_intercom.streams.Admins.get_parent_data")
    @mock.patch("tap_intercom.client.IntercomClient.get")
    def test_admin_get_records(self,mocked_client,mocked_parent_data):
        """
        Verify get_records for Admin stream
        """
        test_stream = Admins(self.base_client, None, ['admins'])

        mocked_parent_data.return_value = ['id']
        mocked_client.return_value = 'test'

        parent_data = list(test_stream.get_records())
        expected_data = ['test']

        self.assertEqual(parent_data,expected_data)

    def test_dt_to_epoch(self):
        """
            Verify expected epoch time with UTC datetime
        """
        date_time_str= "2022-07-28T00:00:00.000000Z"
        converted_datetime = singer.utils.strptime_to_utc(date_time_str)
        expected_epoch = (dt.datetime(2022,7,28,0,0,0) - dt.datetime(1970,1,1)).total_seconds()

        test_epoch = BaseStream.dt_to_epoch_seconds(converted_datetime)

        self.assertEqual(test_epoch,expected_epoch)

    @mock.patch("tap_intercom.client.IntercomClient.get")
    @mock.patch("tap_intercom.streams.LOGGER.critical")
    def test_admin_list_get_records(self,mocked_logger,mocked_client):
        """
        Verify get_records for AdminList
        """
        test_stream = AdminList(self.base_client, None, ['admin_list'])

        mocked_client.return_value = {}

        with self.assertRaises(IntercomError) as e:
            list(test_stream.get_records())

        self.assertEqual(mocked_logger.call_count,1)

    @mock.patch("tap_intercom.client.IntercomClient.post",side_effect =[{'data':[{'key': 'value', 'tags': {}, 'companies': {}}],'pages':{'next':''}}])
    @mock.patch("tap_intercom.streams.BaseStream.dt_to_epoch_seconds")
    def test_contacts_get_records(self,mocked_time,mocked_client):
        """
        Verify get_records for Contacts stream
        """
        test_stream = Contacts(self.base_client, None, ['contacts'])

        test_data = list(test_stream.get_records(stream_metadata={}))
        expected_data = [{'key': 'value'}]

        self.assertEqual(test_data,expected_data)

class TestFullTable(unittest.TestCase):

    base_client = IntercomClient("test","300")

    @mock.patch("tap_intercom.transform.find_datetimes_in_schema")
    @mock.patch("tap_intercom.transform.transform_times")
    @mock.patch("tap_intercom.client.IntercomClient.get")
    def test_sync(self,mocked__client_get,mocked_transform_times,mocked_time):
        """
        Verify sync for a full_table stream
        """
        test_stream = CompanyAttributes(self.base_client, None, ['company_attributes'])
        mocked__client_get.return_value = {'data':[{"key": "value"}],'pages':{'next':''}}
        test_data = test_stream.sync("test","","","","")

        self.assertEqual(test_data,"test")


class TestBaseStreamCoverage(unittest.TestCase):

    base_client = IntercomClient("test", "300")

    def test_get_records_raises_not_implemented(self):
        """BaseStream.get_records raises NotImplementedError."""
        stream = BaseStream()
        with self.assertRaises(NotImplementedError):
            list(stream.get_records())

    def test_incremental_stream_base_set_last_processed(self):
        """IncrementalStream.set_last_processed base sets last_processed to None."""
        stream = Segments(self.base_client, None, [])
        stream.last_processed = 'some-value'
        stream.set_last_processed({})
        self.assertIsNone(stream.last_processed)

    def test_incremental_stream_base_get_last_sync_started_at(self):
        """IncrementalStream.get_last_sync_started_at base sets attribute to None."""
        stream = Segments(self.base_client, None, [])
        stream.last_sync_started_at = 'some-value'
        stream.get_last_sync_started_at({})
        self.assertIsNone(stream.last_sync_started_at)

    def test_incremental_stream_base_set_last_sync_started_at(self):
        """IncrementalStream.set_last_sync_started_at base is a no-op."""
        stream = Segments(self.base_client, None, [])
        stream.set_last_sync_started_at({})  # should not raise

    def test_incremental_stream_base_skip_records_returns_false(self):
        """IncrementalStream.skip_records base always returns False."""
        stream = Segments(self.base_client, None, [])
        self.assertFalse(stream.skip_records({'id': '1'}))

    @mock.patch('singer.write_bookmark', side_effect=singer.write_bookmark)
    def test_incremental_stream_base_write_bookmark(self, _mock):
        """IncrementalStream.write_bookmark base writes to state."""
        stream = Segments(self.base_client, None, [])
        state = {'bookmarks': {}}
        result = stream.write_bookmark(state, '2022-01-01')
        self.assertEqual(
            result['bookmarks']['segments']['updated_at'], '2022-01-01'
        )

    @mock.patch('singer.write_state')
    @mock.patch('singer.write_bookmark', side_effect=singer.write_bookmark)
    def test_incremental_stream_write_intermediate_bookmark_when_enabled(self, _wb, _ws):
        """write_intermediate_bookmark writes when to_write_intermediate_bookmark=True."""
        stream = Contacts(self.base_client, None, [])
        state = {'bookmarks': {}}
        bm_dt = singer.utils.strptime_to_utc('2022-01-01T00:00:00Z')
        stream.write_intermediate_bookmark(state, 'last-id', bm_dt)
        self.assertIn('updated_at', state.get('bookmarks', {}).get('contacts', {}))

    @mock.patch('tap_intercom.client.IntercomClient.get')
    def test_admin_list_get_records_is_parent(self, mocked_get):
        """AdminList.get_records(is_parent=True) yields admin ids."""
        mocked_get.return_value = {'admins': [{'id': 'a1'}, {'id': 'a2'}]}
        stream = AdminList(self.base_client, None, [])
        result = list(stream.get_records(is_parent=True))
        self.assertEqual(result, ['a1', 'a2'])


    @mock.patch('tap_intercom.client.IntercomClient.get')
    def test_companies_get_records_pagination(self, mocked_get):
        """Companies.get_records follows scroll_param pages."""
        mocked_get.side_effect = [
            {'data': [{'id': '1', 'updated_at': 1}], 'scroll_param': 'tok'},
            IntercomNotFoundError(),
        ]
        stream = Companies(self.base_client, None, [])
        result = list(stream.get_records())
        self.assertEqual(len(result), 1)


class TestConversationsBookmarks(unittest.TestCase):

    base_client = IntercomClient("test", "300")

    @mock.patch('singer.write_bookmark', side_effect=singer.write_bookmark)
    def test_write_bookmark_uses_last_sync_started_at(self, _mock):
        """write_bookmark replaces bookmark with last_sync_started_at when present."""
        stream = Conversations(self.base_client, None, [])
        state = {
            'bookmarks': {
                'conversations': {
                    'updated_at': '2022-01-01',
                    'last_sync_started_at': '2022-06-01',
                    'last_processed': 'conv-50',
                }
            }
        }
        result = stream.write_bookmark(state, '2022-01-01')
        self.assertEqual(result['bookmarks']['conversations']['updated_at'], '2022-06-01')
        self.assertNotIn('last_sync_started_at', result['bookmarks']['conversations'])
        self.assertNotIn('last_processed', result['bookmarks']['conversations'])

    @mock.patch('singer.write_state')
    @mock.patch('singer.write_bookmark', side_effect=singer.write_bookmark)
    def test_write_intermediate_bookmark(self, _wb, _ws):
        """write_intermediate_bookmark writes last_processed and last_sync_started_at."""
        stream = Conversations(self.base_client, None, [])
        stream.last_sync_started_at = '2022-06-01'
        state = {'bookmarks': {}}
        stream.write_intermediate_bookmark(state, 'conv-42', None)
        self.assertEqual(
            state['bookmarks']['conversations']['last_processed'], 'conv-42'
        )
        self.assertEqual(
            state['bookmarks']['conversations']['last_sync_started_at'], '2022-06-01'
        )

    @mock.patch('tap_intercom.client.IntercomClient.post')
    def test_get_records_is_parent_yields_ids(self, mocked_post):
        """Conversations.get_records(is_parent=True) yields conversation ids."""
        mocked_post.return_value = {
            'conversations': [{'id': 'c1'}, {'id': 'c2'}],
            'pages': {'next': None},
        }
        stream = Conversations(self.base_client, None, [])
        bookmark = singer.utils.strptime_to_utc('2022-01-01T00:00:00Z')
        result = list(stream.get_records(bookmark_datetime=bookmark, is_parent=True))
        self.assertEqual(result, ['c1', 'c2'])

    @mock.patch('tap_intercom.client.IntercomClient.post')
    def test_get_records_pagination(self, mocked_post):
        """Conversations.get_records follows next-page pagination."""
        mocked_post.side_effect = [
            {
                'conversations': [{'id': 'c1', 'updated_at': 1}],
                'pages': {'next': {'starting_after': 'cursor1'}, 'page': 1},
            },
            {
                'conversations': [{'id': 'c2', 'updated_at': 2}],
                'pages': {},
            },
        ]
        stream = Conversations(self.base_client, None, [])
        bookmark = singer.utils.strptime_to_utc('2022-01-01T00:00:00Z')
        result = list(stream.get_records(bookmark_datetime=bookmark))
        self.assertEqual(len(result), 2)


class TestContactsPagination(unittest.TestCase):

    base_client = IntercomClient("test", "300")

    @mock.patch('tap_intercom.client.IntercomClient.post')
    def test_contacts_get_records_pagination(self, mocked_post):
        """Contacts.get_records follows next-page pagination."""
        mocked_post.side_effect = [
            {
                'data': [{'id': 'u1', 'tags': {}, 'companies': {}}],
                'pages': {'next': {'starting_after': 'cursor1'}, 'page': 1},
            },
            {
                'data': [{'id': 'u2', 'tags': {}, 'companies': {}}],
                'pages': {},
            },
        ]
        stream = Contacts(self.base_client, None, [])
        bookmark = singer.utils.strptime_to_utc('2022-01-01T00:00:00Z')
        result = list(stream.get_records(bookmark_datetime=bookmark, stream_metadata={}))
        self.assertEqual(len(result), 2)
