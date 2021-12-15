
import unittest
from unittest.mock import patch
import requests
from tap_intercom.client import IntercomClient

REQUEST_TIMEOUT_INT = 300
REQUEST_TIMEOUT_STR = "300"
REQUEST_TIMEOUT_FLOAT = 300.0

# Mock response object
def get_mock_http_response(*args, **kwargs):
    contents = '{"access_token": "test", "expires_in":100, "accounts":[{"id": 12}]}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    return response

@patch("time.sleep")
@patch("requests.Session.get", side_effect=requests.exceptions.Timeout)
class TestRequestTimeoutsBackoff(unittest.TestCase):

    def test_request_timeout_backoff_in_check_access_token(self, mocked_request, mocked_sleep):
        """
            Verify check_access_token function is backoff for 5 times on Timeout exceeption
        """
        # Initialize IntercomClient object
        client = IntercomClient('dummy_token', None)
        try:
            client.__enter__()
        except requests.exceptions.Timeout:
            pass

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_request_timeout_backoff_in_request(self, mocked_request, mocked_sleep):
        """
            Verify request function is backoff for 5 times on Timeout exceeption
        """
        # Initialize IntercomClient object
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_INT)# string 0 timeout in config
        try:
            client.request("base_url")
        except requests.exceptions.Timeout:
            pass

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

@patch("time.sleep")
@patch("requests.Session.request", side_effect = get_mock_http_response)
@patch("tap_intercom.client.IntercomClient.check_access_token")
class TestRequestTimeoutsValue(unittest.TestCase):
    def test_no_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
        Verify that if request_timeout is not provided in config then default value is used
        """
        # Initialize IntercomClient object
        client = IntercomClient('dummy_token', None)

        # Call request method which call requests.Session.request with timeout
        client.request("base_url")

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

    def test_integer_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        # Initialize IntercomClient object with request_timeout
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_INT)# int timeout in config
        
        # Call request method which call requests.Session.request with timeout
        client.request("base_url")
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

    def test_float_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        # Initialize IntercomClient object with request_timeout
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_FLOAT)# float timeout in config
        
        # Call request method which call requests.Session.request with timeout
        client.request("base_url")
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

    def test_string_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        # Initialize IntercomClient object with request_timeout
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_STR)# string timeout in config
        
        # Call request method which call requests.Session.request with timeout
        client.request("base_url")
        
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

    def test_empty_string_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        # Initialize IntercomClient object with request_timeout
        client = IntercomClient('dummy_token', "")# empty string timeout in config
        
        # Call request method which call requests.Session.request with timeout
        client.request("base_url")

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

    def test_zero_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        # Initialize IntercomClient object with request_timeout
        client = IntercomClient('dummy_token', 0)# int zero value timeout in config
   
        # Call request method which call requests.Session.request with timeout
        client.request("base_url")

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

    def test_zero_string_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        # Initialize IntercomClient object with request_timeout
        client = IntercomClient('dummy_token', "0")# string 0 timeout in config
     
        # Call request method which call requests.Session.request with timeout
        client.request("base_url")

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument
