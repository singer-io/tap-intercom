from parameterized import parameterized
import unittest
from unittest import mock
import requests,json
from tap_intercom import client
from tap_intercom.client import IntercomClient, IntercomBadRequestError, IntercomError, IntercomUnauthorizedError,\
                                IntercomPaymentRequiredError,IntercomForbiddenError, IntercomNotFoundError, IntercomMethodNotAllowedError,\
                                IntercomNotAcceptableError, IntercomRequestTimeoutError, IntercomUserConflictError, IntercomUnsupportedMediaTypeError,\
                                IntercomUnprocessableEntityError, IntercomScrollExistsError, IntercomRateLimitError, IntercomBadGatewayError,\
                                IntercomGatewayTimeoutError,IntercomServiceUnavailableError,IntercomInternalServiceError, Server5xxError\

def get_mock_http_response(status_code, contents):
    """Returns mock rep"""
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response


class TestCustomErrorHandling(unittest.TestCase):
    """
    Test cases to verify we get custom error messages when we do not recieve error from the API
    """

    intercom_client = client.IntercomClient(config_request_timeout= "", access_token="test_access_token")
    method = 'GET'
    path = 'path'
    url = 'url'
    @parameterized.expand([
        ['400_error', [400,IntercomBadRequestError], 'HTTP-error-code: 400, Error: General client error, possibly malformed data.'],
        ['401_error', [401,IntercomUnauthorizedError], 'HTTP-error-code: 401, Error: The API Key was not authorized (or no API Key was found).'],
        ['402_error', [402,IntercomPaymentRequiredError], 'HTTP-error-code: 402, Error: The API is not available on your current plan.'],
        ['403_error', [403,IntercomForbiddenError], 'HTTP-error-code: 403, Error: The request is not allowed.'],
        ['404_error', [404,IntercomNotFoundError], 'HTTP-error-code: 404, Error: The resource was not found.'],
        ['405_error', [405,IntercomMethodNotAllowedError], 'HTTP-error-code: 405, Error: The resource does not accept the HTTP method.'],
        ['406_error', [406,IntercomNotAcceptableError], 'HTTP-error-code: 406, Error: The resource cannot return the client\'s required content type.'],
        ['408_error', [408,IntercomRequestTimeoutError], 'HTTP-error-code: 408, Error: The server would not wait any longer for the client.'],
        ['409_error', [409,IntercomUserConflictError], 'HTTP-error-code: 409, Error: Multiple existing users match this email address - must be more specific using user_id'],
        ['415_error', [415,IntercomUnsupportedMediaTypeError], 'HTTP-error-code: 415, Error: The server doesn\'t accept the submitted content-type.'],
        ['422_error', [422,IntercomUnprocessableEntityError], 'HTTP-error-code: 422, Error: The data was well-formed but invalid.'],
        ['423_error', [423,IntercomScrollExistsError], 'HTTP-error-code: 423, Error: The source or destination resource of a method is locked.'],
        ['429_error', [429,IntercomRateLimitError], 'HTTP-error-code: 429, Error: The client has reached or exceeded a rate limit.'],
        ['500_error', [500,IntercomInternalServiceError], 'HTTP-error-code: 500, Error: An unhandled error with the Intercom API.'],
        ['502_error', [502,IntercomBadGatewayError], 'HTTP-error-code: 502, Error: Received an invalid response from the upstream server.'],
        ['503_error', [503,IntercomServiceUnavailableError], 'HTTP-error-code: 503, Error: Intercom API service is currently unavailable.'],
        ['504_error', [504,IntercomGatewayTimeoutError], 'HTTP-error-code: 504, Error: The server did not receive a timely response from an upstream server.'],
        ['5xx_error', [509,Server5xxError], 'HTTP-error-code: 509, Error: Unknown Error'],
        ['456_error', [456,IntercomError], 'HTTP-error-code: 456, Error: Unknown Error'],
    ])

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    @mock.patch("tap_intercom.client.IntercomClient.check_access_token")
    def test_custom_response_message(self,name,test_data, expected_data, mocked_api_token, mocked_request, mocked_sleep):
        resp_str = {}
        mocked_request.return_value = get_mock_http_response(test_data[0],json.dumps(resp_str))

        with self.assertRaises(test_data[1]) as e:
            self.intercom_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_data)


class TestResponseErrorHandling(unittest.TestCase):
    """Test cases to verify the error from the API are displayed as expected"""

    client = IntercomClient(config_request_timeout= "", access_token="test_access_token")
    method = 'GET'
    path = 'path'
    url = 'url'
    @parameterized.expand([
        ['400_error', [400, IntercomBadRequestError, {'type': 'error.list','errors': [{'code': 'bad-request', 'message': 'Bad Request.'}]}], 'HTTP-error-code: 400, Error:Bad Request.'],
        ['401_error', [401, IntercomUnauthorizedError, {'type': 'error.list','errors': [{'code': 'unauthorized', 'message': 'Access Token Invalid.'}]}], 'HTTP-error-code: 401, Error:Access Token Invalid.'],
        ['402_error', [402,IntercomPaymentRequiredError, {'type': 'error.list','errors': [{'code': 'payment_required', 'message': 'Current plan does not supports this API.'}]}], 'HTTP-error-code: 402, Error:Current plan does not supports this API.'],
        ['403_error', [403,IntercomForbiddenError, {'type': 'error.list','errors': [{'code': 'forbidden', 'message': 'Forbidden error.'}]}], 'HTTP-error-code: 403, Error:Forbidden error.'],
        ['404_error', [404,IntercomNotFoundError, {'type': 'error.list','errors': [{'code': 'Not Found', 'message': 'Resource not found.'}]}], 'HTTP-error-code: 404, Error:Resource not found.'],
        ['405_error', [405,IntercomMethodNotAllowedError, {'type': 'error.list','errors': [{'code': 'Method Not allowed', 'message': 'Method not accepted.'}]}], 'HTTP-error-code: 405, Error:Method not accepted.'],
        ['406_error', [406,IntercomNotAcceptableError,  {'type': 'error.list','errors': [{'code': 'Not acceptable', 'message': 'Cannot return the requested content type.'}]}], 'HTTP-error-code: 406, Error:Cannot return the requested content type.'],
        ['408_error', [408,IntercomRequestTimeoutError,  {'type': 'error.list','errors': [{'code': 'Timeout Error', 'message': 'Request timeout Error.'}]}], 'HTTP-error-code: 408, Error:Request timeout Error.'],
        ['409_error', [409,IntercomUserConflictError,  {'type': 'error.list','errors': [{'code': 'Conflict Error', 'message': 'User Conflict Error.'}]}], 'HTTP-error-code: 409, Error:User Conflict Error.'],
        ['415_error', [415,IntercomUnsupportedMediaTypeError,  {'type': 'error.list','errors': [{'code': 'Unsupported Media Type', 'message': 'Content-type not supported.'}]}], 'HTTP-error-code: 415, Error:Content-type not supported.'],
        ['422_error', [422,IntercomUnprocessableEntityError,  {'type': 'error.list','errors': [{'code': 'Unproccessable Entity', 'message': 'Data invalid.'}]}], 'HTTP-error-code: 422, Error:Data invalid.'],
        ['423_error', [423,IntercomScrollExistsError,  {'type': 'error.list','errors': [{'code': 'scroll_exists', 'message': 'Server overloaded.'}]}], 'HTTP-error-code: 423, Error:Server overloaded.'],
        ['429_error', [429,IntercomRateLimitError,{'type': 'error.list','errors': [{'code': 'Rate Limit Error', 'message': 'Rate Limit Reached.'}]}], 'HTTP-error-code: 429, Error:Rate Limit Reached.'],
        ['500_error', [500,IntercomInternalServiceError,{'type': 'error.list','errors': [{'code': 'Internal error', 'message': 'Sorry, Unhandled error with the API.'}]}], 'HTTP-error-code: 500, Error:Sorry, Unhandled error with the API.'],
        ['502_error', [502,IntercomBadGatewayError,{'type': 'error.list','errors': [{'code': 'Internal error', 'message': 'Sorry, Internal Error'}]}], 'HTTP-error-code: 502, Error:Sorry, Internal Error'],
        ['503_error', [503,IntercomServiceUnavailableError,{'type': 'error.list','errors': [{'code': 'Service Unavailable', 'message': 'Sorry, Service Unavailable'}]}], 'HTTP-error-code: 503, Error:Sorry, Service Unavailable'],
        ['504_error', [504,IntercomGatewayTimeoutError,{'type': 'error.list','errors': [{'code': 'Service Unavailable', 'message': 'Sorry, No response received'}]}], 'HTTP-error-code: 504, Error:Sorry, No response received'],
        ['Multiple_errors', [400,IntercomBadRequestError,{'type': 'error.list','errors': [{'code': 'test_code_1', 'message': 'Bad_Request_1 error received.'}, {'code': 'test_code_2', 'message': 'Bad_Request_2 error received'}]}], "HTTP-error-code: 400, Error:[{'code': 'test_code_1', 'message': 'Bad_Request_1 error received.'}, {'code': 'test_code_2', 'message': 'Bad_Request_2 error received'}]"],
    ])

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    @mock.patch("tap_intercom.client.IntercomClient.check_access_token")
    def test_response_message(self, name, test_data, expected_data, mocked_api_token, mocked_request, mocked_sleep):
        """
        Exception with response message should be raised if status code returned from API
        """
        resp_str = test_data[2]
        mocked_request.return_value = get_mock_http_response(test_data[0],json.dumps(resp_str))

        with self.assertRaises(test_data[1]) as e:
            self.client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_data)

class TestJsonDecodeError(unittest.TestCase):
    """Test Case to Verify JSON Decode Error"""

    intercom_client = IntercomClient(config_request_timeout= "", access_token="test_access_token")
    method = 'GET'
    path = 'path'
    url = 'url'

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    @mock.patch("tap_intercom.client.IntercomClient.check_access_token")
    def test_json_decode_failed_4XX(self, mocked_api_token, mocked_jsondecode_failure, mocked_sleep):
        """
        Exception with Unknown error message should be raised if invalid JSON response returned with 4XX error
        """
        json_decode_error_str = "json_error"
        mocked_jsondecode_failure.return_value = get_mock_http_response(
            400, json_decode_error_str)

        expected_message = "HTTP-error-code: 400, Error: General client error, possibly malformed data."

        with self.assertRaises(client.IntercomBadRequestError) as e:
            self.intercom_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_message)
