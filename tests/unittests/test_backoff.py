from parameterized import parameterized
import unittest
from unittest import mock
import requests
from tap_intercom.client import IntercomClient, Server5xxError, ConnectionError, IntercomRateLimitError, IntercomScrollExistsError

def get_mock_http_response(status_code, contents):
    """Returns mock rep"""
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response


class TestBackoff(unittest.TestCase):
    """
    Test cases to verify we backoff 7 times for ConnectionError, 5XX errors, 429 error, 423 error
    """
    client = IntercomClient(config_request_timeout= "", access_token="test_access_token")
    method = 'GET'
    path = 'path'
    url = 'url'

    @parameterized.expand([
            ['429_error_backoff', IntercomRateLimitError, None],
            ['Connection_error_backoff', ConnectionError, None],
            ['Server5xx_error_backoff', Server5xxError, None],
            ['423_error_backoff', IntercomScrollExistsError, None],
    ])

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    @mock.patch("tap_intercom.client.IntercomClient.check_access_token")
    def test_backoff(self, name, test_exception, data, mocked_api_cred, mocked_request, mocked_sleep):
        """Test case to verify backoff works as expected"""
        
        mocked_request.side_effect = test_exception('exception')
        with self.assertRaises(test_exception) as e:
            response_json = self.client.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 7)


