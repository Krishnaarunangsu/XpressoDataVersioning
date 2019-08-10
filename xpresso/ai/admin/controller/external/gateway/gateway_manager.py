""" This contains abstract class for API gateway"""
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.external.gateway.kong_api_gateway import \
    KongAPIGateway

__all__ = ['GatewayManager']
__author__ = 'Naveen Sinha'


class GatewayManager:
    """ Manages the gateway transactions """

    CONFIG_GATEWAY_KEY = "gateway"
    CONFIG_GATEWAY_PROVIDER = "provider"
    CONFIG_GATEWAY_ADMIN = "admin_url"
    CONFIG_GATEWAY_PROXY = "proxy_url"

    KONG_GATEWAY = "kong"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH):
        self.config = XprConfigParser(config_file_path=config_path)
        self.api_gateway = None
        self.initialize_gateway(
            gateway_provider=self.config[GatewayManager.CONFIG_GATEWAY_KEY][
                GatewayManager.CONFIG_GATEWAY_PROVIDER],
            admin_url=self.config[GatewayManager.CONFIG_GATEWAY_KEY][
                GatewayManager.CONFIG_GATEWAY_ADMIN],
            proxy_url=self.config[GatewayManager.CONFIG_GATEWAY_KEY][
                GatewayManager.CONFIG_GATEWAY_PROXY],
            config_path=config_path
        )
        self.logger = XprLogger()

    def initialize_gateway(self, gateway_provider, admin_url, proxy_url,
                           config_path):
        if gateway_provider == GatewayManager.KONG_GATEWAY:
            self.api_gateway = KongAPIGateway(
                admin_url=admin_url,
                proxy_url=proxy_url,
                config_path=config_path)

    def setup_external_service(self, component_name, internal_service_url):
        """ Register a service with an external service """

        update_service_url = ["http://" + svc if not svc.startswith("http")
                              else svc for svc in internal_service_url]
        self.logger.info(f"Updated svc: {update_service_url}")
        return self.api_gateway.register_new_service(
            upstream_url=update_service_url,
            service_name=component_name,
            route=f"/{component_name}")

    def delete_external_service(self, component_name):
        return self.api_gateway.delete_service(service_name=component_name)
