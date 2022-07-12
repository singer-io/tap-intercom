from tap_intercom import client
import unittest
from unittest import mock

class MockResponse:
    """ Returns Mock Response"""
    def __init__(self,  status_code, json):
        self.status_code = status_code
        self.text = json
        self.links = {}

    def json(self):
        return self.text

def get_response(status_code, json={}):
    """ Returns mock response class"""

    return MockResponse(status_code, json)

@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
class TestErrorHandling(unittest.TestCase):
    """
    Test cases to verify if the errors are handled as expected while communicating with Intercom Environment
    """

    intercom_client = client.IntercomClient(config_request_timeout= "", access_token="test_access_token")
    method = 'GET'
    path = 'path'
    url = 'url'

    def test_400_custom_response_message(self, mocked_400_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 400 status code returned from API
        """
        resp_str = {}
        mocked_400_successful.return_value = get_response(400, resp_str)

        expected_message = "HTTP-error-code: 400, Error: General client error, possibly malformed data."

        with self.assertRaises(client.IntercomBadRequestError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_401_custom_response_message(self, mocked_401_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 401 status code returned from API
        """
        resp_str = {}
        mocked_401_successful.return_value = get_response(401, resp_str)

        expected_message = "HTTP-error-code: 401, Error: The API Key was not authorized (or no API Key was found)."

        with self.assertRaises(client.IntercomUnauthorizedError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_403_custom_response_message(self, mocked_403_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 403 status code returned from API
        """
        resp_str = {}
        mocked_403_successful.return_value = get_response(403, resp_str)

        expected_message = "HTTP-error-code: 403, Error: The request is not allowed."

        with self.assertRaises(client.IntercomForbiddenError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_404_custom_response_message(self, mocked_404_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 404 status code returned from API
        """
        resp_str = {}
        mocked_404_successful.return_value = get_response(404, resp_str)

        expected_message = "HTTP-error-code: 404, Error: The resource was not found."

        with self.assertRaises(client.IntercomNotFoundError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
   
    def test_405_custom_response_message(self, mocked_405_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 405 status code returned from API
        """
        resp_str = {}
        mocked_405_successful.return_value = get_response(405, resp_str)

        expected_message = "HTTP-error-code: 405, Error: The resource does not accept the HTTP method."

        with self.assertRaises(client.IntercomMethodNotAllowedError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_406_custom_response_message(self, mocked_406_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 406 status code returned from API
        """
        resp_str = {}
        mocked_406_successful.return_value = get_response(406, resp_str)

        expected_message = "HTTP-error-code: 406, Error: The resource cannot return the client's required content type."

        with self.assertRaises(client.IntercomNotAcceptableError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_408_custom_response_message(self, mocked_408_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 408 status code returned from API
        """
        resp_str = {}
        mocked_408_successful.return_value = get_response(408, resp_str)

        expected_message = "HTTP-error-code: 408, Error: The server would not wait any longer for the client."

        with self.assertRaises(client.IntercomRequestTimeoutError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_409_custom_response_message(self, mocked_409_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 409 status code returned from API
        """
        resp_str = {}
        mocked_409_successful.return_value = get_response(409, resp_str)

        expected_message = "HTTP-error-code: 409, Error: Multiple existing users match this email address - must be more specific using user_id"

        with self.assertRaises(client.IntercomUserConflictError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_415_custom_response_message(self, mocked_415_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 415 status code returned from API
        """
        resp_str = {}
        mocked_415_successful.return_value = get_response(415, resp_str)

        expected_message = "HTTP-error-code: 415, Error: The server doesn't accept the submitted content-type."

        with self.assertRaises(client.IntercomUnsupportedMediaTypeError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_422_custom_response_message(self, mocked_422_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 422 status code returned from API
        """
        resp_str = {}
        mocked_422_successful.return_value = get_response(422, resp_str)

        expected_message = "HTTP-error-code: 422, Error: The data was well-formed but invalid."

        with self.assertRaises(client.IntercomUnprocessableEntityError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_423_custom_response_message(self, mocked_423_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 423 status code returned from API
        """
        resp_str = {}
        mocked_423_successful.return_value = get_response(423, resp_str)

        expected_message = "HTTP-error-code: 423, Error: The client has reached or exceeded a rate limit, or the server is overloaded."

        with self.assertRaises(client.IntercomScrollExistsError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_423_successful.call_count, 7)
    
    def test_500_custom_response_message(self, mocked_500_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 500 status code returned from API
        """
        resp_str = {}
        mocked_500_successful.return_value = get_response(500, resp_str)

        expected_message = "HTTP-error-code: 500, Error: An unhandled error with the Intercom API."

        with self.assertRaises(client.IntercomInternalServiceError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_502_custom_response_message(self, mocked_502_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 502 status code returned from API
        """
        resp_str = {}
        mocked_502_successful.return_value = get_response(502, resp_str)

        expected_message = "HTTP-error-code: 502, Error: Received an invalid response from the upstream server."

        with self.assertRaises(client.IntercomBadGatewayError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_503_custom_response_message(self, mocked_503_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 503 status code returned from API
        """
        resp_str = {}
        mocked_503_successful.return_value = get_response(503, resp_str)

        expected_message = "HTTP-error-code: 503, Error: Intercom API service is currently unavailable."

        with self.assertRaises(client.IntercomServiceUnavailableError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
   
    def test_504_custom_response_message(self, mocked_504_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 504 status code returned from API
        """
        resp_str = {}
        mocked_504_successful.return_value = get_response(504, resp_str)

        expected_message = "HTTP-error-code: 504, Error: The server did not receive a timely response from an upstream server."

        with self.assertRaises(client.IntercomGatewayTimeoutError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_unknown_error_response_message(self, mocked_504_successful, mocked_sleep):
        """
        Exception with unknown message should be raised if unknown status code returned from API
        """
        resp_str = {}
        mocked_504_successful.return_value = get_response(456, resp_str)

        expected_message = "HTTP-error-code: 456, Error: Unknown Error"

        with self.assertRaises(client.IntercomError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)

    def test_400_Error_response_message(self, mocked_400_successful, mocked_sleep):
        """
        Exception with error message should be raised if 400 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'bad-request', 'message': 'Bad Request.'}]}
        mocked_400_successful.return_value = get_response(400, resp_str)

        expected_message = "HTTP-error-code: 400, Error:Bad Request."

        with self.assertRaises(client.IntercomBadRequestError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
            
        self.assertEqual(str(e.exception), expected_message)
    
    def test_401_Error_response_message(self, mocked_401_successful, mocked_sleep):
        """
        Exception with error message should be raised if 401 status code returned from API
            """
        resp_str = {'type': 'error.list','errors': [{'code': 'unauthorized', 'message': 'Access Token Invalid.'}]}
        mocked_401_successful.return_value = get_response(401, resp_str)

        expected_message = "HTTP-error-code: 401, Error:Access Token Invalid."

        with self.assertRaises(client.IntercomUnauthorizedError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_403_Error_response_message(self, mocked_403_successful, mocked_sleep):
        """
        Exception with error message should be raised if 403 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'forbidden', 'message': 'Forbidden error.'}]}
        mocked_403_successful.return_value = get_response(403, resp_str)

        expected_message = "HTTP-error-code: 403, Error:Forbidden error."

        with self.assertRaises(client.IntercomForbiddenError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_404_Error_response_message(self, mocked_404_successful, mocked_sleep):
        """
        Exception with error message should be raised if 404 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Not Found', 'message': 'Resource not found.'}]}
        mocked_404_successful.return_value = get_response(404, resp_str)

        expected_message = "HTTP-error-code: 404, Error:Resource not found."

        with self.assertRaises(client.IntercomNotFoundError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_405_Error_response_message(self, mocked_405_successful, mocked_sleep):
        """
        Exception with error message should be raised if 405 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Method Not allowed', 'message': 'Method not accepted.'}]}
        mocked_405_successful.return_value = get_response(405, resp_str)

        expected_message = "HTTP-error-code: 405, Error:Method not accepted."

        with self.assertRaises(client.IntercomMethodNotAllowedError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_406_Error_response_message(self, mocked_406_successful, mocked_sleep):
        """
        Exception with error message should be raised if 406 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Not acceptable', 'message': 'Cannot return the requested content type.'}]}
        mocked_406_successful.return_value = get_response(406, resp_str)

        expected_message = "HTTP-error-code: 406, Error:Cannot return the requested content type."

        with self.assertRaises(client.IntercomNotAcceptableError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_408_Error_response_message(self, mocked_408_successful, mocked_sleep):
        """
        Exception with error message should be raised if 408 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Timeout Error', 'message': 'Request timeout Error.'}]}
        mocked_408_successful.return_value = get_response(408, resp_str)

        expected_message = "HTTP-error-code: 408, Error:Request timeout Error."

        with self.assertRaises(client.IntercomRequestTimeoutError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)

    def test_409_Error_response_message(self, mocked_409_successful, mocked_sleep):
        """
        Exception with error message should be raised if 409 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Conflict Error', 'message': 'User Conflict Error.'}]}
        mocked_409_successful.return_value = get_response(409, resp_str)

        expected_message = "HTTP-error-code: 409, Error:User Conflict Error."

        with self.assertRaises(client.IntercomUserConflictError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_415_Error_response_message(self, mocked_415_successful, mocked_sleep):
        """
        Exception with error message should be raised if 415 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Unsupported Media Type', 'message': 'Content-type not supported.'}]}
        mocked_415_successful.return_value = get_response(415, resp_str)

        expected_message = "HTTP-error-code: 415, Error:Content-type not supported."

        with self.assertRaises(client.IntercomUnsupportedMediaTypeError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_422_Error_response_message(self, mocked_422_successful, mocked_sleep):
        """
        Exception with error message should be raised if 422 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Unproccessable Entity', 'message': 'Data invalid.'}]}
        mocked_422_successful.return_value = get_response(422, resp_str)

        expected_message = "HTTP-error-code: 422, Error:Data invalid."

        with self.assertRaises(client.IntercomUnprocessableEntityError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_423_Error_response_message(self, mocked_423_successful, mocked_sleep):
        """
        Exception with error message should be raised if 423 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Scroll exist error', 'message': 'Server overloaded.'}]}
        mocked_423_successful.return_value = get_response(423, resp_str)

        expected_message = "HTTP-error-code: 423, Error:Server overloaded."

        with self.assertRaises(client.IntercomScrollExistsError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_500_Error_response_message(self, mocked_500_successful, mocked_sleep):
        """
        Exception with error message should be raised if 500 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Internal error', 'message': 'Internal Service error.'}]}
        mocked_500_successful.return_value = get_response(500, resp_str)

        expected_message = "HTTP-error-code: 500, Error:Internal Service error."

        with self.assertRaises(client.IntercomInternalServiceError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_502_Error_response_message(self, mocked_502_successful, mocked_sleep):
        """
        Exception with error message should be raised if 502 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Bad Gateway', 'message': 'Bad Gateway error.'}]}
        mocked_502_successful.return_value = get_response(502, resp_str)

        expected_message = "HTTP-error-code: 502, Error:Bad Gateway error."

        with self.assertRaises(client.IntercomBadGatewayError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_503_Error_response_message(self, mocked_503_successful, mocked_sleep):
        """
        Exception with error message should be raised if 503 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Service Unavailable', 'message': 'Service is unavailable.'}]}
        mocked_503_successful.return_value = get_response(503, resp_str)

        expected_message = "HTTP-error-code: 503, Error:Service is unavailable."

        with self.assertRaises(client.IntercomServiceUnavailableError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)

    def test_504_Error_response_message(self, mocked_504_successful, mocked_sleep):
        """
        Exception with error message should be raised if 504 status code returned from API
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'Gateway Timeout', 'message': 'Gateway timeout error.'}]}
        mocked_504_successful.return_value = get_response(504, resp_str)

        expected_message = "HTTP-error-code: 504, Error:Gateway timeout error."

        with self.assertRaises(client.IntercomGatewayTimeoutError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)
    
    def test_multiple_Error_response_message(self, mocked_504_successful, mocked_sleep):
        """
        Exception with multiple error messages should be raised if `errors` object contains multiple messages
        """
        resp_str = {'type': 'error.list','errors': [{'code': 'test_code_1', 'message': 'Bad_Request_1 error received.'}, {'code': 'test_code_2', 'message': 'Bad_Request_2 error received'}]}
        mocked_504_successful.return_value = get_response(400, resp_str)

        expected_message = "HTTP-error-code: 400, Error:[{'code': 'test_code_1', 'message': 'Bad_Request_1 error received.'}, {'code': 'test_code_2', 'message': 'Bad_Request_2 error received'}]"

        with self.assertRaises(client.IntercomBadRequestError) as e:
            self.intercom_client.request(self.method, self.path, self.url)
        
        self.assertEqual(str(e.exception), expected_message)