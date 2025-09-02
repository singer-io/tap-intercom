import unittest
from singer.transform import unix_milliseconds_to_datetime
from tap_intercom.transform import get_integer_places, transform_json, find_datetimes_in_schema
from parameterized import parameterized


def test_get_integer_places():

    input_output_mapping = {
        -62135596800: 10,
        62135596800: 11,
        1612998233: 10,
        -1: 10,
        -9999999999999: 10,
        -999999999999997: 10,
        1612998430000: 13,
        9999999999999999: 16,
    }

    for input_values, expected_values in input_output_mapping.items():
        assert expected_values == get_integer_places(input_values)


def test_unix_milliseconds_to_datetime():

    input_output_mapping = {
        -62135596800: "1968-01-12T20:06:43.200000Z",
        62135596800: "1971-12-21T03:53:16.800000Z",
        99999999999999: "5138-11-16T09:46:39.998993Z",
        -1: "1969-12-31T23:59:59.999000Z",
    }
    for input_values, expected_values in input_output_mapping.items():
        assert expected_values == unix_milliseconds_to_datetime(input_values)

class TestTransform(unittest.TestCase):

    @parameterized.expand([ # test_name, stream_name, expected_json
        ['stream_users','users',[{'test':'val'}]],
        ['stream_companies','companies',[{'test':'val'}]],
        ['stream_conversations','conversations',[{'test':'val'}]]
    ])
    def test_transform_and_denest(self,test_name,test_stream,exp):
        """Test transform and denest for different streams"""

        this_json = {'key':exp}
        stream_name = test_stream
        data_key = 'key'

        # Call the transform function
        json = transform_json(this_json,stream_name,data_key)
        self.assertEqual(json,exp)

    @parameterized.expand([ # test_name, schema_data, expected_data
        ["having_properties",{ "properties": {"created_at": {"format": "date-time"}}}, [['created_at']]],
        ["having_items",{ "items":{"properties": {"created_at": {"format": "date-time"}}}}, [[['created_at']]]],
        ["having_nested_properties",{ "properties": {"nest":{"data":"value","properties":{"created_at": {"format": "date-time"}}}}}, [['nest','created_at']]],
        ["having_nested_items",{ "items":{"properties": {"nest":{"data":"value","items":{"properties":{"created_at": {"format": "date-time"}}}}}}}, [['nest',['created_at']]]]
    ])
    def test_find_datetimes(self,test_name,test_schema,exp):
        """Test to find datetime in schema having different scenarios """

        # Call the datetime function
        path = find_datetimes_in_schema(test_schema)
        self.assertEqual(path,exp)
