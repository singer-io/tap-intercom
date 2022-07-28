from unittest import mock
from tap_intercom.client import IntercomClient, IntercomBadRequestError
import unittest
import requests,json
from parameterized import parameterized

def get_mock_http_response(status_code, contents):
    """Returns mock rep"""
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response


class TestResponse(unittest.TestCase):

    
    intercom_client = IntercomClient(config_request_timeout= "", access_token="")
    
    @parameterized.expand([['200_True',[200,IntercomBadRequestError,{'type':''}],True],
                        ['200_False',[200,IntercomBadRequestError,{}],False]
    ])

    @mock.patch("requests.Session.request")
    def test_response(self,name ,test_data, expected_data, mocked_true_successful):
        """ Verify for 'type' received in response"""

        resp_str = test_data[2]
        mocked_true_successful.return_value = get_mock_http_response(test_data[0], json.dumps(resp_str))

        response= self.intercom_client.check_access_token()
        
        self.assertEqual(response, expected_data)
        self.assertEqual(mocked_true_successful.call_count, 1)
    
    
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_400_response_message(self,mocked_400_successful, mocked_sleep):
        """
        Verify status_code != 200 and raises appropriate error in check_access_token()
        """
        resp_str = {}
        mocked_400_successful.return_value = get_mock_http_response(400, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 400, Error: General client error, possibly malformed data."

        with self.assertRaises(IntercomBadRequestError) as e:
            self.intercom_client.check_access_token()

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_400_successful.call_count, 1)

class TestMethods(unittest.TestCase):
    
    intercom_client = IntercomClient(config_request_timeout= "", access_token="test_access_token")

    @parameterized.expand([
        ['POST_Method','POST',[1,0]],
        ['GET_Method','GET',[0,1]],
    ])

    @mock.patch("tap_intercom.client.IntercomClient.post")
    @mock.patch("tap_intercom.client.IntercomClient.get")
    def test_post_perform(self,name, test_method, expected_count, mocked_get_request,mocked_post_request):
        """ Verify if method == POST/GET, POST/GET call is returned respectively"""

        self.intercom_client.perform(test_method,'path')
        self.assertEqual(mocked_post_request.call_count,expected_count[0])
        self.assertEqual(mocked_get_request.call_count,expected_count[1])
    
    @mock.patch("tap_intercom.client.IntercomClient.get")
    def test_get(self,mocked_get_request):
        """ Verify GET method """

        self.intercom_client.get('GET','path')
        self.assertEqual(mocked_get_request.call_count,1)

    @mock.patch("tap_intercom.client.IntercomClient.post")
    def test_post(self,mocked_post_request):
        """ Verify POST method """

        self.intercom_client.perform('POST','path')
        self.assertEqual(mocked_post_request.call_count,1)
