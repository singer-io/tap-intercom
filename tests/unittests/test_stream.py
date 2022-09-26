import unittest
import singer
import datetime as dt
from unittest import mock
from tap_intercom.client import IntercomClient, IntercomError
from parameterized import parameterized
from tap_intercom.streams import AdminList, BaseStream, Admins,Companies, CompanyAttributes,Contacts, ContactAttributes,CompnaySegments, Conversations, Segments, Tags, Teams,ConversationParts

class TestData(unittest.TestCase):
    
    base_client = IntercomClient("test","300")
    @parameterized.expand([
        ['Companies',[Companies, [{'data':'', 'scroll_param':''},{'data':''}]], []],
        ['CompanyAttributes',[CompanyAttributes, [{'data':['test1'],'pages':{'next':'abc'}},{'data':['test2'],'pages':{'next':''}}]],['test1','test2']],
        ['CompanySegments',[CompnaySegments,[{'segments':['test1'],'pages':{'next':'abc'}},{'segments':['test2'],'pages':{'next':''}}]],['test1','test2']],
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
        
        test_stream = data[0](self.base_client)
        mocked_client.side_effect = data[1]

        test_data = list(test_stream.get_records())
        self.assertEqual(test_data,expected_data)
    
    @mock.patch("tap_intercom.streams.BaseStream.get_parent_data")
    @mock.patch("tap_intercom.client.IntercomClient.get")
    def test_admin_get_records(self,mocked_client,mocked_parent_data):
        """
        Verify get_records for Admin stream
        """
        test_stream = Admins(self.base_client)
        
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
        test_stream = AdminList(self.base_client)
        
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
        test_stream = Contacts(self.base_client)

        test_data = list(test_stream.get_records(metadata={}))
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
        test_stream = CompanyAttributes(self.base_client)
        mocked__client_get.return_value = {'data':[{"key": "value"}],'pages':{'next':''}}
        test_data = test_stream.sync("test","","","","")

        self.assertEqual(test_data,"test")