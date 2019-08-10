"""
Processes the response from a requests
"""
from json import JSONDecodeError

__all__ = ['HTTPResponse']
__author__ = 'Naveen Sinha'

import json


class HTTPResponse:
    """ Plain object for storing the HTTP response parameters

    Args:
        response_code:
        response_data:
        response_headers:

    """

    def __init__(self, response_code: int, response_data,
                 response_headers: dict):
        self.code = response_code
        self.data = response_data
        self.headers = response_headers

    def ok(self):
        """
        Check if the response is succesfull
        """
        if 200 <= self.code < 300:
            return True
        return False

    def get_data_as_json(self):
        """ convert data into json and sends back. Returns None if not a valid
        dict"""
        try:
            json_data = json.loads(self.data)
        except (JSONDecodeError, TypeError):
            json_data = None
        return json_data

    def get_raw_data(self):
        """ send back raw data """
        return self.data

    def __str__(self):
        return f""" Statuc Code: {self.code}\nHeaders: {self.headers}\n
        Data: {self.data}"""
