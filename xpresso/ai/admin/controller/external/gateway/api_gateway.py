""" This contains abstract class for API gateway"""


__all__ = ['APIGateway']
__author__ = 'Naveen Sinha'

from abc import abstractmethod

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class APIGateway:
    """
    This abstract class is to perform following service:
        1. Register a new route in the gateway service
        2. Modify the route in the gateway service
        3. Get route details
        4. Get metrics on a route
    """

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH):
        self.xpr_config = XprConfigParser(
            config_file_path=config_path)
        self.logger = XprLogger()

    @abstractmethod
    def register_new_service(self,upstream_url, service_name, route):
        """ Register a new service into the gateway"""

    @abstractmethod
    def delete_service(self, service_name):
        """ Delete an existing service from the gateway"""

    @abstractmethod
    def get_service(self, service_name):
        """ Get an existing service from the gateway"""

    @abstractmethod
    def get_metrics(self, service_name):
        """ Get an existing service from the gateway"""
