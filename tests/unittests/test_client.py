from unittest import mock
from tap_intercom.client import IntercomClient, IntercomError, IntercomBadRequestError
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

        with self.assertRaises(IntercomError) as e:
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


class TestProbeStreamAndMethods(unittest.TestCase):

    intercom_client = IntercomClient(config_request_timeout="", access_token="test_token")

    @mock.patch("tap_intercom.client.IntercomClient.get")
    @mock.patch("tap_intercom.client.IntercomClient.post")
    def test_perform_calls_get_by_default(self, mock_post, mock_get):
        self.intercom_client.perform('GET', 'path')
        mock_get.assert_called_once()
        mock_post.assert_not_called()

    @mock.patch("tap_intercom.client.IntercomClient.get")
    @mock.patch("tap_intercom.client.IntercomClient.post")
    def test_perform_calls_post_for_post_method(self, mock_post, mock_get):
        self.intercom_client.perform('POST', 'path')
        mock_post.assert_called_once()
        mock_get.assert_not_called()

    @mock.patch("requests.Session.request")
    def test_get_method_delegates_to_request(self, mock_request):
        mock_request.return_value = get_mock_http_response(200, json.dumps({'type': ''}))
        client = IntercomClient(config_request_timeout="", access_token="test_token")
        client._IntercomClient__verified = True
        client.get('test_path')
        self.assertEqual(mock_request.call_args[0][0], 'GET')

    @mock.patch("requests.Session.request")
    def test_post_method_delegates_to_request(self, mock_request):
        mock_request.return_value = get_mock_http_response(200, json.dumps({'type': ''}))
        client = IntercomClient(config_request_timeout="", access_token="test_token")
        client._IntercomClient__verified = True
        client.post('test_path', json={})
        self.assertEqual(mock_request.call_args[0][0], 'POST')

class TestContextManagerAndAccessToken(unittest.TestCase):
    """Covers IntercomClient.__enter__/__exit__ and check_access_token edge cases."""

    @mock.patch("tap_intercom.client.IntercomClient.check_access_token", return_value=True)
    def test_enter_returns_self_and_exit_closes_session(self, mocked_check):
        """`with IntercomClient(...) as client` must yield the same instance."""
        client = IntercomClient(config_request_timeout="", access_token="test_token")
        with client as ctx:
            self.assertIs(ctx, client)
        mocked_check.assert_called_once()

    def test_check_access_token_raises_when_access_token_missing(self):
        """check_access_token() must raise when access_token is None."""
        client = IntercomClient(config_request_timeout="", access_token=None)
        with self.assertRaises(Exception) as ctx:
            client.check_access_token()
        self.assertIn('Missing access_token', str(ctx.exception))

    @mock.patch("requests.Session.request")
    def test_check_access_token_sets_user_agent_header(self, mock_request):
        """check_access_token() must include the configured User-Agent header."""
        mock_request.return_value = get_mock_http_response(200, json.dumps({'type': ''}))
        client = IntercomClient(config_request_timeout="", access_token="test_token", user_agent="my-tap-agent")
        result = client.check_access_token()
        self.assertTrue(result)
        sent_headers = mock_request.call_args.kwargs.get('headers', {})
        self.assertEqual(sent_headers.get('User-Agent'), 'my-tap-agent')

    @mock.patch("requests.Session.request")
    def test_request_pops_endpoint_kwarg(self, mock_request):
        """The 'endpoint' kwarg must be consumed for metrics and not leak to requests."""
        mock_request.return_value = get_mock_http_response(200, json.dumps({'type': ''}))
        client = IntercomClient(config_request_timeout="", access_token="test_token")
        client._IntercomClient__verified = True
        client.get('test_path', endpoint='some_stream')
        self.assertNotIn('endpoint', mock_request.call_args.kwargs)

    @mock.patch("requests.Session.request")
    def test_request_sets_user_agent_header(self, mock_request):
        """request() must include the configured User-Agent header."""
        mock_request.return_value = get_mock_http_response(200, json.dumps({'type': ''}))
        client = IntercomClient(config_request_timeout="", access_token="test_token", user_agent="my-tap-agent")
        client._IntercomClient__verified = True
        client.get('test_path')
        self.assertEqual(mock_request.call_args.kwargs['headers'].get('User-Agent'), 'my-tap-agent')