from tap_intercom import LOGGER, client
import unittest
from unittest import mock
import requests,json

def get_mock_http_response(status_code, contents):
    """Returns mock rep"""
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response

@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
@mock.patch("tap_intercom.client.IntercomClient.check_access_token")
class TestErrorHandling(unittest.TestCase):
    """
    Test cases to verify if the errors are handled as expected while communicating with Intercom Environment
    """

    intercom_client = client.IntercomClient(config_request_timeout= "", access_token="test_access_token")
    method = 'GET'
    path = 'path'
    url = 'url'

    def test_400_custom_response_message(self, mocked_api_token, mocked_400_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 400 status code returned from API
        """
        resp_str = {}
        mocked_400_successful.return_value = get_mock_http_response(400, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 400, Error: General client error, possibly malformed data."

        with self.assertRaises(client.IntercomBadRequestError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_400_successful.call_count, 1)
    
    def test_401_custom_response_message(self, mocked_api_token, mocked_401_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 401 status code returned from API
        """
        resp_str = {}
        mocked_401_successful.return_value = get_mock_http_response(401, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 401, Error: The API Key was not authorized (or no API Key was found)."

        with self.assertRaises(client.IntercomUnauthorizedError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_401_successful.call_count, 1)
    
    def test_402_custom_response_message(self, mocked_api_token, mocked_402_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 402 status code returned from API
        """
        resp_str = {}
        mocked_402_successful.return_value = get_mock_http_response(402, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 402, Error: The API is not available on your current plan."

        with self.assertRaises(client.IntercomPaymentRequiredError) as e:
            self.intercom_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_402_successful.call_count, 1)

    def test_403_custom_response_message(self, mocked_api_token, mocked_403_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 403 status code returned from API
        """
        resp_str = {}
        mocked_403_successful.return_value = get_mock_http_response(403, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 403, Error: The request is not allowed."

        with self.assertRaises(client.IntercomForbiddenError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_403_successful.call_count, 1)
    
    def test_404_custom_response_message(self, mocked_api_token, mocked_404_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 404 status code returned from API
        """
        resp_str = {}
        mocked_404_successful.return_value = get_mock_http_response(404, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 404, Error: The resource was not found."

        with self.assertRaises(client.IntercomNotFoundError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_404_successful.call_count, 1)

    def test_405_custom_response_message(self, mocked_api_token, mocked_405_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 405 status code returned from API
        """
        resp_str = {}
        mocked_405_successful.return_value = get_mock_http_response(405, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 405, Error: The resource does not accept the HTTP method."

        with self.assertRaises(client.IntercomMethodNotAllowedError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_405_successful.call_count, 1)
    
    def test_406_custom_response_message(self, mocked_api_token, mocked_406_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 406 status code returned from API
        """
        resp_str = {}
        mocked_406_successful.return_value = get_mock_http_response(406, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 406, Error: The resource cannot return the client's required content type."

        with self.assertRaises(client.IntercomNotAcceptableError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_406_successful.call_count, 1)
    
    def test_408_custom_response_message(self, mocked_api_token, mocked_408_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 408 status code returned from API
        """
        resp_str = {}
        mocked_408_successful.return_value = get_mock_http_response(408, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 408, Error: The server would not wait any longer for the client."

        with self.assertRaises(client.IntercomRequestTimeoutError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_408_successful.call_count, 1)
    
    def test_409_custom_response_message(self, mocked_api_token, mocked_409_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 409 status code returned from API
        """
        resp_str = {}
        mocked_409_successful.return_value = get_mock_http_response(409, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 409, Error: Multiple existing users match this email address - must be more specific using user_id"

        with self.assertRaises(client.IntercomUserConflictError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_409_successful.call_count, 1)
    
    def test_415_custom_response_message(self, mocked_api_token, mocked_415_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 415 status code returned from API
        """
        resp_str = {}
        mocked_415_successful.return_value = get_mock_http_response(415, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 415, Error: The server doesn't accept the submitted content-type."

        with self.assertRaises(client.IntercomUnsupportedMediaTypeError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_415_successful.call_count, 1)
    
    def test_422_custom_response_message(self, mocked_api_token, mocked_422_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 422 status code returned from API
        """
        resp_str = {}
        mocked_422_successful.return_value = get_mock_http_response(422, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 422, Error: The data was well-formed but invalid."

        with self.assertRaises(client.IntercomUnprocessableEntityError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_422_successful.call_count, 1)

    def test_423_custom_response_message(self, mocked_api_token, mocked_423_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 423 status code returned from API
        """
        resp_str = {}
        mocked_423_successful.return_value = get_mock_http_response(423, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 423, Error: The source or destination resource of a method is locked."

        with self.assertRaises(client.IntercomScrollExistsError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_423_successful.call_count, 7)
    
    def test_429_custom_response_message(self, mocked_api_token, mocked_429_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 429 status code returned from API
        """
        resp_str = {}
        mocked_429_successful.return_value = get_mock_http_response(429, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 429, Error: The client has reached or exceeded a rate limit."

        with self.assertRaises(client.IntercomRateLimitError) as e:
            self.intercom_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_429_successful.call_count, 7)

    def test_500_custom_response_message(self,mocked_api_token, mocked_500_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 500 status code returned from API
        """
        resp_str = {}
        mocked_500_successful.return_value = get_mock_http_response(500, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 500, Error: An unhandled error with the Intercom API."

        with self.assertRaises(client.IntercomInternalServiceError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_500_successful.call_count, 7)
    
    def test_502_custom_response_message(self, mocked_api_token, mocked_502_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 502 status code returned from API
        """
        resp_str = {}
        mocked_502_successful.return_value = get_mock_http_response(502, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 502, Error: Received an invalid response from the upstream server."

        with self.assertRaises(client.IntercomBadGatewayError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_502_successful.call_count, 7)
    
    def test_503_custom_response_message(self,mocked_api_token, mocked_503_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 503 status code returned from API
        """
        resp_str = {}
        mocked_503_successful.return_value = get_mock_http_response(503, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 503, Error: Intercom API service is currently unavailable."

        with self.assertRaises(client.IntercomServiceUnavailableError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_503_successful.call_count, 7)

    def test_504_custom_response_message(self,mocked_api_token, mocked_504_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 504 status code returned from API
        """
        resp_str = {}
        mocked_504_successful.return_value = get_mock_http_response(504, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 504, Error: The server did not receive a timely response from an upstream server."

        with self.assertRaises(client.IntercomGatewayTimeoutError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_504_successful.call_count, 7)

    def test_5xx_custom_response_message(self, mocked_api_token, mocked_5xx_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 5XX status code returned from API and 'message' not present in response
        """
        resp_str = {}
        mocked_5xx_successful.return_value = get_mock_http_response(509, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 509, Error: Unknown Error"

        with self.assertRaises(client.Server5xxError) as e:
            self.intercom_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_5xx_successful.call_count, 7)
    
    def test_unknown_error_response_message(self, mocked_api_token, mocked_456_successful, mocked_sleep):
        """
        Exception with unknown message should be raised if unknown status code returned from API
        """
        resp_str = {}
        mocked_456_successful.return_value = get_mock_http_response(456, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 456, Error: Unknown Error"

        with self.assertRaises(client.IntercomError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_456_successful.call_count, 1)

    def test_400_Error_response_message(self, mocked_api_token, mocked_400_successful, mocked_sleep):
        """
        Exception with error message should be raised if 400 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'bad-request', 'message': 'Bad Request.'}]}
        mocked_400_successful.return_value = get_mock_http_response(400, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 400, Error:Bad Request."

        with self.assertRaises(client.IntercomBadRequestError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
            
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_400_successful.call_count, 1)
    
    def test_401_Error_response_message(self, mocked_api_token, mocked_401_successful, mocked_sleep):
        """
        Exception with error message should be raised if 401 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'unauthorized', 'message': 'Access Token Invalid.'}]}
        mocked_401_successful.return_value = get_mock_http_response(401, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 401, Error:Access Token Invalid."

        with self.assertRaises(client.IntercomUnauthorizedError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_401_successful.call_count, 1)
    
    def test_402_Error_response_message(self, mocked_api_token, mocked_402_successful, mocked_sleep):
        """
        Exception with error message should be raised if 402 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'payment_required', 'message': 'Current plan does not supports this API.'}]}
        mocked_402_successful.return_value = get_mock_http_response(402, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 402, Error:Current plan does not supports this API."

        with self.assertRaises(client.IntercomPaymentRequiredError) as e:
            self.intercom_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_402_successful.call_count, 1)

    def test_403_Error_response_message(self, mocked_api_token, mocked_403_successful, mocked_sleep):
        """
        Exception with error message should be raised if 403 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'forbidden', 'message': 'Forbidden error.'}]}
        mocked_403_successful.return_value = get_mock_http_response(403, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 403, Error:Forbidden error."

        with self.assertRaises(client.IntercomForbiddenError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_403_successful.call_count, 1)
    
    def test_404_Error_response_message(self, mocked_api_token, mocked_404_successful, mocked_sleep):
        """
        Exception with error message should be raised if 404 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Not Found', 'message': 'Resource not found.'}]}
        mocked_404_successful.return_value = get_mock_http_response(404, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 404, Error:Resource not found."

        with self.assertRaises(client.IntercomNotFoundError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_404_successful.call_count, 1)
    
    def test_405_Error_response_message(self, mocked_api_token, mocked_405_successful, mocked_sleep):
        """
        Exception with error message should be raised if 405 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Method Not allowed', 'message': 'Method not accepted.'}]}
        mocked_405_successful.return_value = get_mock_http_response(405, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 405, Error:Method not accepted."

        with self.assertRaises(client.IntercomMethodNotAllowedError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_405_successful.call_count, 1)
    
    def test_406_Error_response_message(self, mocked_api_token, mocked_406_successful, mocked_sleep):
        """
        Exception with error message should be raised if 406 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Not acceptable', 'message': 'Cannot return the requested content type.'}]}
        mocked_406_successful.return_value = get_mock_http_response(406, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 406, Error:Cannot return the requested content type."

        with self.assertRaises(client.IntercomNotAcceptableError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_406_successful.call_count, 1)
    
    def test_408_Error_response_message(self, mocked_api_token, mocked_408_successful, mocked_sleep):
        """
        Exception with error message should be raised if 408 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Timeout Error', 'message': 'Request timeout Error.'}]}
        mocked_408_successful.return_value = get_mock_http_response(408, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 408, Error:Request timeout Error."

        with self.assertRaises(client.IntercomRequestTimeoutError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_408_successful.call_count, 1)

    def test_409_Error_response_message(self, mocked_api_token, mocked_409_successful, mocked_sleep):
        """
        Exception with error message should be raised if 409 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Conflict Error', 'message': 'User Conflict Error.'}]}
        mocked_409_successful.return_value = get_mock_http_response(409, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 409, Error:User Conflict Error."

        with self.assertRaises(client.IntercomUserConflictError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_409_successful.call_count, 1)
    
    def test_415_Error_response_message(self, mocked_api_token, mocked_415_successful, mocked_sleep):
        """
        Exception with error message should be raised if 415 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Unsupported Media Type', 'message': 'Content-type not supported.'}]}
        mocked_415_successful.return_value = get_mock_http_response(415, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 415, Error:Content-type not supported."

        with self.assertRaises(client.IntercomUnsupportedMediaTypeError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_415_successful.call_count, 1)
    
    def test_422_Error_response_message(self, mocked_api_token, mocked_422_successful, mocked_sleep):
        """
        Exception with error message should be raised if 422 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Unproccessable Entity', 'message': 'Data invalid.'}]}
        mocked_422_successful.return_value = get_mock_http_response(422, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 422, Error:Data invalid."

        with self.assertRaises(client.IntercomUnprocessableEntityError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_422_successful.call_count, 1)
    
    def test_423_Error_response_message(self, mocked_api_token, mocked_423_successful, mocked_sleep):
        """
        Exception with error message should be raised if 423 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'scroll_exists', 'message': 'Server overloaded.'}]}
        mocked_423_successful.return_value = get_mock_http_response(423, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 423, Error:Server overloaded."

        with self.assertRaises(client.IntercomScrollExistsError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_423_successful.call_count, 7)
    
    def test_429_Error_response_message(self, mocked_api_token, mocked_429_successful, mocked_sleep):
        """
        Exception with error message should be raised if 429 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Rate Limit Error', 'message': 'Rate Limit Reached.'}]}
        mocked_429_successful.return_value = get_mock_http_response(429, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 429, Error:Rate Limit Reached."

        with self.assertRaises(client.IntercomRateLimitError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_429_successful.call_count, 7)
    
    def test_500_Error_response_message(self, mocked_api_token, mocked_500_successful, mocked_sleep):
        """
        Exception with error message should be raised if 500 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Internal error', 'message': 'Sorry, Unhandled error with the API.'}]}
        mocked_500_successful.return_value = get_mock_http_response(500, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 500, Error:Sorry, Unhandled error with the API."

        with self.assertRaises(client.IntercomInternalServiceError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_500_successful.call_count, 7)
    
    def test_502_Error_response_message(self, mocked_api_token, mocked_502_successful, mocked_sleep):
        """
        Exception with error message should be raised if 502 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Internal error', 'message': 'Sorry, Internal Error'}]}
        mocked_502_successful.return_value = get_mock_http_response(502, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 502, Error:Sorry, Internal Error"

        with self.assertRaises(client.IntercomBadGatewayError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_502_successful.call_count, 7)

    def test_503_Error_response_message(self, mocked_api_token, mocked_503_successful, mocked_sleep):
        """
        Exception with error message should be raised if 503 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Service Unavailable', 'message': 'Sorry, Service Unavailable'}]}
        mocked_503_successful.return_value = get_mock_http_response(503, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 503, Error:Sorry, Service Unavailable"

        with self.assertRaises(client.IntercomServiceUnavailableError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_503_successful.call_count, 7)
    
    def test_504_Error_response_message(self, mocked_api_token, mocked_504_successful, mocked_sleep):
        """
        Exception with error message should be raised if 504 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Service Unavailable', 'message': 'Sorry, No response received'}]}
        mocked_504_successful.return_value = get_mock_http_response(504, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 504, Error:Sorry, No response received"

        with self.assertRaises(client.IntercomGatewayTimeoutError) as e:
            self.intercom_client.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_504_successful.call_count, 7)

    def test_multiple_Error_response_message(self, mocked_api_token, mocked_400_successful, mocked_sleep):
        """
        Exception with multiple error messages should be raised if `errors` object contains multiple messages
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'test_code_1', 'message': 'Bad_Request_1 error received.'}, {'code': 'test_code_2', 'message': 'Bad_Request_2 error received'}]}
        mocked_400_successful.return_value = get_mock_http_response(400, json.dumps(resp_str))

        expected_message = "HTTP-error-code: 400, Error:[{'code': 'test_code_1', 'message': 'Bad_Request_1 error received.'}, {'code': 'test_code_2', 'message': 'Bad_Request_2 error received'}]"

        with self.assertRaises(client.IntercomBadRequestError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_400_successful.call_count, 1)

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
