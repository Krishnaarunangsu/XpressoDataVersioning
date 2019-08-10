"Setting up the Logstash as a docker service"

__all__ = ['LogstashSetup']
__author__ = 'Srijan Sharma'

import sys
import os
import docker
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.utils.linux_utils import check_root
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    PermissionDeniedException

class LogstashSetup:
    """
    This class is used to setup the logstash server as a docker container.
    """

    LOGSTASH_SECTION="logstash"
    LOGSTASH_IMAGE="logstash_image"
    LOGSTASH_CONTAINER="logstash_container"
    LOGSTASH_CONF="conf_abs_path"
    LOGSTASH_PORT="logstash_port"
    ELASTIC_SEARCH_IP="elastic_search_ip"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH_SETUP_LOG):
        self.config = XprConfigParser(config_path)
        self.client = docker.from_env()

    def install(self):
        if not check_root():
            print("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")

        self.setup_logstash()

    def setup_logstash(self):
        if not check_root():
            print("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")

        if not os.path.exists(self.config[self.LOGSTASH_SECTION][self.LOGSTASH_CONF]):
            print("Unable to find the logstash config folder at mentioned path")
            raise FileNotFoundError

        try:
            self.pull_image()
            self.update_logstash_config()
            self.client.containers.run(self.config[self.LOGSTASH_SECTION][self.LOGSTASH_IMAGE],
                                       volumes={self.config[self.LOGSTASH_SECTION][self.LOGSTASH_CONF]: {'bind': '/usr/share/logstash/config/', 'mode': 'rw'}},
                                       name=self.config[self.LOGSTASH_SECTION][self.LOGSTASH_CONTAINER],
                                       ports={"5000":self.config[self.LOGSTASH_SECTION][self.LOGSTASH_PORT],"9600":"9300"},
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
            self.client.images.pull(self.config[self.LOGSTASH_SECTION][self.LOGSTASH_IMAGE])
        except docker.errors.APIError as err:
            print("The server returns an error. \n{}".format(str(err)))
            raise err

    def update_logstash_config(self):
        files_to_update = ["xpresso_format.yml","logstash_format.yml"]

        for file in files_to_update:
            try:
                with open(os.path.join(self.config[self.LOGSTASH_SECTION][self.LOGSTASH_CONF],file),"r") as f:
                    newlines = []
                    for line in f.readlines():
                        newlines.append(line.replace("##IP##",self.config[self.LOGSTASH_SECTION][self.ELASTIC_SEARCH_IP]))
                    f.close()

                write_filename = file.replace("_format","")
                with open(os.path.join(self.config[self.LOGSTASH_SECTION][self.LOGSTASH_CONF],write_filename),"w") as w:
                    for line in newlines:
                        w.write(line)
                    w.close()
            except EnvironmentError as err:
                print("Unable to update the logstash config files with the elastic search IP . \n{}".format(str(err)))
            except FileNotFoundError as err:
                print("Unable to file the config file in base directory. Loading from the config "
                      "from default path. \n{}".format(str(err)))
                raise err
        return True

if __name__ == '__main__':
    ls = LogstashSetup()
    ls.install()
