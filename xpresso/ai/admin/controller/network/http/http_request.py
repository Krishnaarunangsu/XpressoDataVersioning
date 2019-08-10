"""
Plain Object to store single request data
"""

__all__ = ['HTTPRequest', 'HTTPMethod']
__author__ = 'Naveen Sinha'

from enum import Enum


class HTTPMethod(Enum):
    """
    Enum class to specify http method
    """
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class HTTPRequest:
    """ Plain object which stores all the relevant information of a request

    Args:
        method
        url
        headers
        data
    """

    def __init__(self, method: HTTPMethod, url: str,
                 headers: dict = None, data=None):
        self.method = method
        self.url = url
        self.headers = headers
        self.data = data

    def __str__(self):
        return f""" Method: {self.method}\nURL: {self.url}\n
        Headers: {self.headers}\n Data: {self.data}"""
