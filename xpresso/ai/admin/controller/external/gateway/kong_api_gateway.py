""" Kong API Gateway service """

__all__ = ["KongAPIGateway"]
__author__ = "Naveen Sinha"

import os
import requests
from simplejson.errors import JSONDecodeError

from xpresso.ai.admin.controller.exceptions.xpr_exceptions import APIGatewayDuplicateExceptions
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import APIGatewayExceptions
from xpresso.ai.admin.controller.external.gateway.api_gateway import APIGateway
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class KongAPIGateway(APIGateway):
    """ Manages all transaction related to
    Kong gateway service """

    def __init__(self, admin_url, proxy_url,
                 config_path=XprConfigParser.DEFAULT_CONFIG_PATH):
        super().__init__(config_path=config_path)
        self.admin_url = admin_url
        self.proxy_url = proxy_url

    def register_new_service(self, upstream_url, service_name, route):
        """ Registers a new service to the Kong API Gateway.
        It first creates a service for the upstream url and then create
        a path based rule

        Returns:
            str: Return the external IP
        """
        try:
            self.create_service(upstream_url=upstream_url,
                                service_name=service_name)
            self.create_path_route(service_name=service_name,
                                   route=route)
        except APIGatewayDuplicateExceptions:
            self.logger.info("Same services already exist")
        except APIGatewayExceptions as e:
            self.logger.info("deleting stale services")
            self.delete_service(service_name=service_name)
            raise APIGatewayExceptions(e.message)
        new_route = os.path.join(self.proxy_url, route.strip("/"))
        return new_route

    def create_service(self, upstream_url, service_name):
        """ Use Kong REST API to create a service"""
        post_raw_data = {
            "name": service_name,
            "url": upstream_url,
            "tags": service_name
        }
        service_url = os.path.join(self.admin_url, "services")
        self.send_post_request(post_raw_data, service_url)

    @staticmethod
    def get_route_name(service_name):
        """ Generate route name for a service """
        return f"{service_name}_route"

    def create_path_route(self, service_name, route):
        """ Use Kong REST API to create a service"""
        post_raw_data = {
            "name": self.get_route_name(service_name),
            "paths": [route]
        }
        route_url = os.path.join(self.admin_url, "services",
                                 service_name, "routes")
        self.send_post_request(post_raw_data, route_url)

    def send_post_request(self, post_raw_data, api_url):
        """ Generic function to send post request"""
        self.logger.debug(f"Posting {post_raw_data} to {api_url}")
        post_response = requests.post(api_url, data=post_raw_data)
        if post_response.status_code == 409:
            self.logger.error(f"{post_response.status_code}:{post_response.text}")
            raise APIGatewayDuplicateExceptions(message="Duplicate Service.")
        elif post_response.status_code >= 400:
            self.logger.error(f"{post_response.status_code}:{post_response.text}")
            raise APIGatewayExceptions(message="Service can not be accessed")
        self.logger.debug(f"Received response: {post_response.text}")

    def delete_service(self, service_name):
        """ Delete service and route and return if
        they are successful """
        return {
            "service": self.get_json_data(
                request_type=requests.delete,
                url=os.path.join(self.admin_url, "services", service_name)),
            "route": self.get_json_data(
                request_type=requests.delete,
                url=os.path.join(self.admin_url, "routes", self.get_route_name(service_name)))
        }

    def get_json_data(self, request_type, url):
        """ Fetch JSON data from external REST API """
        self.logger.debug(f"Sending GET Request to {url}")
        resp = request_type(url)
        resp_json = {}
        if resp.status_code >= 400:
            self.logger.error(f"{resp.status_code}:{resp.text}")
            return resp_json
        self.logger.debug(f"Received response: {resp.text}")
        try:
            resp_json = resp.json()
        except JSONDecodeError:
            self.logger.error("Invalid JSON receives")
        return resp_json

    def get_service(self, service_name):
        """ Get service information """
        return {
            "service": self.get_json_data(
                request_type=requests.get,
                url=os.path.join(self.admin_url, "services", service_name)),
            "route": self.get_json_data(
                request_type=requests.get,
                url=os.path.join(self.admin_url, "routes", self.get_route_name(service_name)))
        }

    def get_metrics(self, service_name):
        """ Fetch metrics for a service name"""
        return {}
