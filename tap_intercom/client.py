import backoff
import requests
import singer
from requests.exceptions import ConnectionError, Timeout
from simplejson.scanner import JSONDecodeError
from singer import metrics, utils

LOGGER = singer.get_logger()

API_VERSION = '2.5'

REQUEST_TIMEOUT = 300

class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class IntercomError(Exception):
    pass


class IntercomBadRequestError(IntercomError):
    pass


class IntercomBadResponseError(IntercomError):
    pass


class IntercomScrollExistsError(IntercomError):
    pass


class IntercomUnauthorizedError(IntercomError):
    pass


class IntercomPaymentRequiredError(IntercomError):
    pass


class IntercomForbiddenError(IntercomError):
    pass


class IntercomNotFoundError(IntercomError):
    pass


class IntercomMethodNotAllowedError(IntercomError):
    pass


class IntercomNotAcceptableError(IntercomError):
    pass


class IntercomRequestTimeoutError(IntercomError):
    pass


class IntercomConflictError(IntercomError):
    pass


class IntercomUserConflictError(IntercomError):
    pass


class IntercomUnsupportedMediaTypeError(IntercomError):
    pass


class IntercomUnprocessableEntityError(IntercomError):
    pass


class IntercomInternalServiceError(Server5xxError):
    pass


class IntercomRateLimitError(Server429Error):
    pass


class IntercomBadGatewayError(Server5xxError):
    pass


class IntercomServiceUnavailableError(Server5xxError):
    pass


class IntercomGatewayTimeoutError(Server5xxError):
    pass


# Error codes: https://developers.intercom.com/intercom-api-reference/reference/http-responses
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": IntercomBadRequestError,
        "message": "General client error, possibly malformed data."
    },
    401: {
        "raise_exception": IntercomUnauthorizedError,
        "message": "The API Key was not authorized (or no API Key was found)."
    },
    402: {
        "raise_exception": IntercomPaymentRequiredError,
        "message": "The API is not available on your current plan."
    },
    403: {
        "raise_exception": IntercomForbiddenError,
        "message": "The request is not allowed."
    },
    404: {
        "raise_exception": IntercomNotFoundError,
        "message": "The resource was not found."
    },
    405: {
        "raise_exception": IntercomMethodNotAllowedError,
        "message": "The resource does not accept the HTTP method."
    },
    406: {
        "raise_exception": IntercomNotAcceptableError,
        "message": "The resource cannot return the client's required content type."
    },
    408: {
        "raise_exception": IntercomRequestTimeoutError,
        "message": "The server would not wait any longer for the client."
    },
    409: {
        "raise_exception": IntercomUserConflictError,
        "message": "Multiple existing users match this email address - must be more specific using user_id"
    },
    415: {
        "raise_exception": IntercomUnsupportedMediaTypeError,
        "message": "The server doesn't accept the submitted content-type."
    },
    422: {
        "raise_exception": IntercomUnprocessableEntityError,
        "message": "The data was well-formed but invalid."
    },
    423: {
        "raise_exception": IntercomScrollExistsError,
        "message": "The source or destination resource of a method is locked."
    },
    429: {
        "raise_exception": IntercomRateLimitError,
        "message": "The client has reached or exceeded a rate limit."
    },
    500: {
        "raise_exception": IntercomInternalServiceError,
        "message": "An unhandled error with the Intercom API."
    },
    502: {
        "raise_exception": IntercomBadGatewayError,
        "message": "Received an invalid response from the upstream server."
    },
    503: {
        "raise_exception": IntercomServiceUnavailableError,
        "message": "Intercom API service is currently unavailable."
    },
    504: {
        "raise_exception": IntercomGatewayTimeoutError,
        "message": "The server did not receive a timely response from an upstream server."
    }}


def get_exception_for_error_code(error_code, intercom_error_code):
    """Maps the error_code to respective error_message """

    if intercom_error_code == 'scroll_exists':
        error_code = 423

    exception = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception")
    if not exception:
        exception = Server5xxError if error_code >= 500 else IntercomError
    return exception

def raise_for_error(response):
    """Raises error class with appropriate msg for the response"""
    try:
        response_json = response.json() # Retrieve json response
    except Exception: # pylint: disable=broad-except
        response_json = {}
    errors = response_json.get('errors', [])
    status_code = response.status_code
    intercom_error_code = ''

    #  https://developers.intercom.com/intercom-api-reference/reference/error-objects
    if errors: # Response containing `errors` object

        if len(errors) > 1:
            message = "HTTP-error-code: {}, Error:{}".format(status_code,errors)
        else:
            err_msg = errors[0].get('message')
            intercom_error_code = errors[0].get('code')
            message = "HTTP-error-code: {}, Error:{}, Error_Code:{}".format(status_code,err_msg,intercom_error_code)
    else:
        # Prepare custom default error message
        message = "HTTP-error-code: {}, Error: {}".format(status_code,
                response_json.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(
                status_code, {}).get("message", "Unknown Error")))

    exc = get_exception_for_error_code(error_code = status_code, intercom_error_code = intercom_error_code)

    raise exc(message) from None

class IntercomClient(object):
    def __init__(self,
                 access_token,
                 config_request_timeout, # request_timeout parameter
                 user_agent=None):
        self.__access_token = access_token
        self.__user_agent = user_agent
        # Rate limit initial values, reset by check_access_token headers
        self.__session = requests.Session()
        self.__verified = False
        self.base_url = 'https://api.intercom.io'

        # Set request timeout to config param `request_timeout` value.
        # If value is 0,"0","" or not passed then it set default to 300 seconds.
        if config_request_timeout and float(config_request_timeout):
            self.__request_timeout = float(config_request_timeout)
        else:
            self.__request_timeout = REQUEST_TIMEOUT

    # `check_access_token` may throw timeout error. `request` method also call `check_access_token`.
    # So, to add backoff over `check_access_token` may cause 5*5 = 25 times backoff which is not expected.
    # That's why added backoff here.
    @backoff.on_exception(backoff.expo, Timeout, max_tries=5, factor=2)
    def __enter__(self):
        self.__verified = self.check_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, IntercomRateLimitError),
                          max_tries=7,
                          factor=3)
    @utils.ratelimit(1000, 60)
    def check_access_token(self):
        if self.__access_token is None:
            raise Exception('Error: Missing access_token.')
        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Authorization'] = 'Bearer {}'.format(self.__access_token)
        headers['Accept'] = 'application/json'
        headers['Intercom-Version'] = API_VERSION
        response = self.__session.get(
            # Simple endpoint that returns 1 Account record (to check API/access_token access):
            url='{}/{}'.format(self.base_url, 'tags'),
            timeout=self.__request_timeout, # Pass request timeout
            headers=headers)
        if response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            raise_for_error(response)
        else:
            resp = response.json()
            if 'type' in resp:
                return True
            return False

    # Rate limiting:
    #  https://developers.intercom.com/intercom-api-reference/reference#rate-limiting
    @backoff.on_exception(backoff.expo, Timeout, max_tries=5, factor=2) # Backoff for request timeout
    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, IntercomBadResponseError, IntercomRateLimitError, IntercomScrollExistsError),
                          max_tries=7,
                          factor=3)
    @utils.ratelimit(1000, 60)
    def request(self, method, path=None, url=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access_token()

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        LOGGER.info("URL: {} {}, Params: {}, JSON Body: {}".format(method, url, kwargs.get("params"), kwargs.get("json")))

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)
        kwargs['headers']['Accept'] = 'application/json'
        kwargs['headers']['Intercom-Version'] = API_VERSION

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, timeout=self.__request_timeout, **kwargs) # Pass request timeout
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code != 200:
            raise_for_error(response)

        # Sometimes a 200 status code is returned with no content, which breaks JSON decoding.
        try:
            return response.json()
        except JSONDecodeError as err:
            raise IntercomBadResponseError from err

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)

    def perform(self, method, path, **kwargs):
        if method=='POST':
            return self.post(path, **kwargs)
        return self.get(path, **kwargs)
