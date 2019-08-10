"Setting up the Elastic search as a docker service"

__all__ = ['ElasticSetup']
__author__ = 'Srijan Sharma'

import sys
import docker
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.utils.linux_utils import check_root
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    PermissionDeniedException

class ElasticSetup:
    """
    This class is used to setup elastic search service as a docker container.
    """

    ELASTIC_SECTION = "elastic_search"
    ELASTIC_IMAGE = "elastic_image"
    ELASTIC_CONTAINER = "elastic_container"
    ELASTIC_PORT = "elastic_port"
    ELASTIC_SEARCH_DUMP_NAME="elastic_search_dump_name"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH_SETUP_LOG):
        self.config = XprConfigParser(config_path)
        self.client = docker.from_env()

    def install(self):
        if not check_root():
            print("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")
        self.setup_elastic_search()

    def setup_elastic_search(self):
        if not check_root():
            print("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")

        try:
            self.pull_image()
            self.client.containers.run(self.config[self.ELASTIC_SECTION][self.ELASTIC_IMAGE],
                                       volumes={self.config[self.ELASTIC_SECTION][self.ELASTIC_SEARCH_DUMP_NAME]: {'bind': '/usr/share/elasticsearch/data/', 'mode': 'rw'}},
                                       environment={"discovery.type":"single-node"}, ports={"9200":self.config[self.ELASTIC_SECTION][self.ELASTIC_PORT]},
                                       name= self.config[self.ELASTIC_SECTION][self.ELASTIC_CONTAINER], detach=True,
                                       restart_policy={"Name": "on-failure", "MaximumRetryCount": 5})
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
            self.client.images.pull(self.config[self.ELASTIC_SECTION][self.ELASTIC_IMAGE])
        except docker.errors.APIError as err:
            print("The server returns an error. \n{}".format(str(err)))
            raise  err

if __name__=="__main__":
    es = ElasticSetup()
    es.install()