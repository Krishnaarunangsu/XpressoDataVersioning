"""
Manages the HTTP request/response
"""

__all__ = ['HTTPHandler']
__author__ = 'Naveen Sinha'

import requests

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.network.http.http_request import HTTPRequest
from xpresso.ai.admin.controller.network.http.http_request import HTTPMethod
from xpresso.ai.admin.controller.network.http.http_response import HTTPResponse
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    HTTPRequestFailedException
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    HTTPInvalidRequestException


class HTTPHandler:
    """
    Manages the HTTP request and response for external queries.
    """

    def __init__(self):
        self.logger = XprLogger()
        self.empty_response = HTTPResponse(500, {}, {})

    def send_request(self, request: HTTPRequest) -> HTTPResponse:
        """
        Sends the HTTP request and gets the response

        Args:
            request(HTTPRequest): Object of HTTP Request which contains
                                  necessary request data

        Returns:
            HTTPResponse: response object
        """

        try:
            self.logger.info(f"Sending HTTP Request: {request}")
            if request.method == HTTPMethod.GET:
                response = requests.get(request.url,
                                        json=request.data,
                                        headers=request.headers)
            elif request.method == HTTPMethod.POST:
                response = requests.post(request.url,
                                         json=request.data,
                                         headers=request.headers)
            elif request.method == HTTPMethod.PUT:
                response = requests.put(request.url,
                                        json=request.data,
                                        headers=request.headers)
            elif request.method == HTTPMethod.DELETE:
                response = requests.delete(request.url,
                                           json=request.data,
                                           headers=request.headers)
            elif request.method == HTTPMethod.HEAD:
                response = requests.head(request.url,
                                         json=request.data,
                                         headers=request.headers)
            elif request.method == HTTPMethod.OPTIONS:
                response = requests.options(request.url,
                                            json=request.data,
                                            headers=request.headers)
            else:
                raise HTTPInvalidRequestException("Invalid HTTP Method")

            parsed_response = self.parse_response(response)
            self.logger.info(f"Received HTTP Request: {parsed_response}")
        except (requests.HTTPError, requests.ConnectionError,
                requests.ConnectTimeout, requests.exceptions.SSLError) as e:
            self.logger.error(e)
            raise HTTPRequestFailedException("Request Failed")
        except (requests.exceptions.InvalidHeader,
                requests.exceptions.InvalidSchema,
                requests.exceptions.InvalidURL) as e:
            self.logger.error(e)
            raise HTTPInvalidRequestException("Invalid Request Object")
        return parsed_response

    def parse_response(self, response: requests.Response) -> HTTPResponse:
        """ Convert requests.Response into HTTP Response object"""
        if not response:
            return self.empty_response
        return HTTPResponse(response_code=response.status_code,
                            response_data=response.text,
                            response_headers=response.headers)
