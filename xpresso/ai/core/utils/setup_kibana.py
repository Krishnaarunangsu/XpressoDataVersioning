"Setting up the Kibana as a docker service"

__all__ = ['KibanaSetup']
__author__ = 'Srijan Sharma'

import sys
import os
import docker
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.utils.linux_utils import check_root
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import\
    PermissionDeniedException

class KibanaSetup:
    """
    This class is used to  setup the Kibana Server as a docker container.
    """
    KIBANA_SECTION="kibana"
    KIBANA_IMAGE="kibana_image"
    KIBANA_CONTAINER="kibana_container"
    KIBANA_PORT="kibana_port"
    ELASTIC_SEARCH_IP="elastic_search_ip"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH_SETUP_LOG):
        self.config = XprConfigParser(config_path)
        self.client = docker.from_env()

    def install(self):
        if not check_root():
            print("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")

        self.setup_kibana()

    def setup_kibana(self):
        if not check_root():
            print("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")

        try:
            self.pull_image()
            self.client.containers.run(self.config[self.KIBANA_SECTION][self.KIBANA_IMAGE],
                                       environment={"ELASTICSEARCH_URL": self.config[self.KIBANA_SECTION][self.ELASTIC_SEARCH_IP]},
                                       ports={"5601":self.config[self.KIBANA_SECTION][self.KIBANA_PORT]}, name= self.config[self.KIBANA_SECTION][self.KIBANA_CONTAINER],
                                       detach=True, restart_policy={"Name": "on-failure", "MaximumRetryCount": 5})
        except docker.errors.ContainerError as err:
            print("The container exits with a non-zero exit code. \n{}".format(str(err)))
            raise err
        except docker.errors.ImageNotFound as err:
            print("The specified image does not exist. \n{}".format(str(err)))
            raise err
        except docker.errors.APIError as err:
            print("The server returns an error. \n{}".format(str(err)))
            raise err
        except KeyError as err:
            print("Key not present. \n{}".format(str(err)))
            raise err
        return

    def pull_image(self):
        if not check_root():
            print("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")

        try:
            self.client.images.pull(self.config[self.KIBANA_SECTION][self.KIBANA_IMAGE])
        except docker.errors.APIError as err:
            print("The server returns an error. \n{}".format(str(err)))
            raise  err

if __name__=="__main__":
    ks = KibanaSetup()
    ks.install()
