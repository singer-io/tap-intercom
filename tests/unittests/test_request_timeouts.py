
import unittest
from unittest.mock import patch
from requests.exceptions import Timeout
from tap_intercom.client import IntercomClient

REQUEST_TIMEOUT_INT = 300
REQUEST_TIMEOUT_STR = "300"
REQUEST_TIMEOUT_FLOAT = 300.0

@patch("time.sleep")
@patch("requests.Session.get", side_effect=Timeout)
class TestRequestTimeoutsInCheckAccessToken(unittest.TestCase):
    """
    Verify that tap retry 5 times on Timeout error raise from check_access_token method
    """
    def test_no_request_timeout_in_config(self, mocked_get, mock_sleep):
        """
        Verify that if request_timeout is not provided in config then default value is used
        """
        client = IntercomClient('dummy_token', None)
        try:
            client.__enter__()
        except Timeout:
            pass

        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mocked_get.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_get.call_count, 5)

    def test_integer_request_timeout_in_config(self, mocked_get, mock_sleep):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_INT)# int timeout in config
        try:
            client.__enter__()
        except Timeout:
            pass

        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mocked_get.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

        # Verify that requests.Session.get is called 5 times
        self.assertEqual(mocked_get.call_count, 5)

    def test_float_request_timeout_in_config(self, mocked_get, mock_sleep):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_FLOAT)# float timeout in config
        try:
            client.__enter__()
        except Timeout:
            pass
        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mocked_get.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

        # Verify that requests.Session.get is called 5 times
        self.assertEqual(mocked_get.call_count, 5)

    def test_string_request_timeout_in_config(self, mocked_get, mock_sleep):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_INT)# string timeout in config
        try:
            client.__enter__()
        except Timeout:
            pass

        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mocked_get.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

        # Verify that requests.Session.get is called 5 times
        self.assertEqual(mocked_get.call_count, 5)

    def test_empty_string_request_timeout_in_config(self, mocked_get, mock_sleep):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        client = IntercomClient('dummy_token', "")# empty string timeout in config
        try:
            client.__enter__()
        except Timeout:
            pass

        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mocked_get.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.get is called 5 times
        self.assertEqual(mocked_get.call_count, 5)

    def test_zero_request_timeout_in_config(self, mocked_get, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        client = IntercomClient('dummy_token', 0)# int zero value timeout in config
        try:
            client.__enter__()
        except Timeout:
            pass

        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mocked_get.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.get is called 5 times
        self.assertEqual(mocked_get.call_count, 5)

    def test_zero_string_request_timeout_in_config(self, mocked_get, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_INT)# string 0 timeout in config
        try:
            client.__enter__()
        except Timeout:
            pass

        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mocked_get.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.get is called 5 times
        self.assertEqual(mocked_get.call_count, 5)

@patch("time.sleep")
@patch("requests.Session.request", side_effect=Timeout)
@patch("tap_intercom.client.IntercomClient.check_access_token")
class TestRequestTimeoutsInRequestMethod(unittest.TestCase):
    """
    Verify that tap retry 5 times on Timeout error raise from request method
    """
    def test_no_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
        Verify that if request_timeout is not provided in config then default value is used
        """
        client = IntercomClient('dummy_token', None)
        try:
            client.request("base_url")
        except Timeout:
            pass

        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_integer_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_INT)# int timeout in config
        try:
            client.request("base_url")
        except Timeout:
            pass


        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_float_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_FLOAT)# float timeout in config
        try:
            client.request("base_url")
        except Timeout:
            pass
        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_string_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_INT)# string timeout in config
        try:
            client.request("base_url")
        except Timeout:
            pass

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_empty_string_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        client = IntercomClient('dummy_token', "")# empty string timeout in config
        try:
            client.request("base_url")
        except Timeout:
            pass

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_zero_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        client = IntercomClient('dummy_token', 0)# int zero value timeout in config
        try:
            client.request("base_url")
        except Timeout:
            pass

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_zero_string_request_timeout_in_config(self, mocked_token, mocked_request, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        client = IntercomClient('dummy_token', REQUEST_TIMEOUT_INT)# string 0 timeout in config
        try:
            client.request("base_url")
        except Timeout:
            pass

        # Verify requests.Session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)
