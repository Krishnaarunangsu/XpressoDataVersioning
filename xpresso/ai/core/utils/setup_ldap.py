"Setting up the LDAP as a docker service"

__all__ = ['LdapSetup']
__author__ = 'Srijan Sharma'

import os
import docker
import ldap
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.utils.linux_utils import check_root
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    PermissionDeniedException
from xpresso.ai.admin.controller.authentication.ldap_manager import LdapManager
from xpresso.ai.core.logging.xpr_log import XprLogger

class LdapSetup:
    """
    This class is used to setup the LDAP server as a docker container.
    """

    LDAP_SECTION="ldap"
    LDAP_IMAGE="ldap_image"
    LDAP_CONTAINER="ldap_container"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH_SETUP_LOG):
        self.config = XprConfigParser(config_path)
        self.logger = XprLogger()
        self.ldapmanager = LdapManager()
        self.client = docker.from_env()

    def install(self):
        if not check_root():
            self.logger.error("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")

        self.setup_ldap()

    def setup_ldap(self):
        if not check_root():
            self.logger.error("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")

        try:
            self.pull_image()
            self.client.containers.run(self.config[self.LDAP_SECTION][self.LDAP_IMAGE],
                                       name=self.config[self.LDAP_SECTION][self.LDAP_CONTAINER],
                                       environment={
                                           "LDAP_ORGANISATION":"abzooba",
                                           "LDAP_DOMAIN": "abzooba.com",
                                           "LDAP_ADMIN_PASSWORD":"admin"},
                                       ports={"389":"389","636":"636"},
                                       detach=True, restart_policy={"Name": "on-failure", "MaximumRetryCount": 5})
            self.logger.info("Ldap docker service successfully started")
        except docker.errors.ContainerError as err:
            self.logger.error("The container exits with a non-zero exit code. \n{}".format(str(err)))
            raise err
        except docker.errors.ImageNotFound as err:
            self.logger.error("The specified image does not exist. \n{}".format(str(err)))
            raise err
        except docker.errors.APIError as err:
            self.logger.error("The server returns an error. \n{}".format(str(err)))
            raise err
        except KeyError as err:
            self.logger.error("Key not present. \n{}".format(str(err)))
            raise err
        return

    def pull_image(self):
        if not check_root():
            self.logger.error("Please run the program using sudo privileges")
            raise PermissionDeniedException("Permission Denied. Run with admin rights.")

        try:
            self.client.images.pull(self.config[self.LDAP_SECTION][self.LDAP_IMAGE])
            self.logger.info("Successfully pulled LDAP docker image.")
        except docker.errors.APIError as err:
            self.logger.error("The server returns an error. \n{}".format(str(err)))
            raise err

    def insert_default_users(self):
        users = {
            "admin_user" : {
                "uid" : "xprdb_admin",
                "pwd" : 'xprdb@Abz00ba'
            },
            "superuser" : {
                "uid" : "superuser1",
                "pwd" : "xprdb@Abz00ba"
            },
            "admin1_user" : {
                "uid" : "admin1",
                "pwd" : "admin1"
            }
        }

        self.logger.info("Creating default users in LDAP")

        for key in users:
            try:
                self.ldapmanager.add(users[key]["uid"],users[key]["pwd"])
                self.logger.info("Successfully added {} .".format(key))
            except ldap.INVALID_CREDENTIALS  as e:
                self.logger.error("Error : {} in adding ".format(str(e),key))
            except ldap.LDAPError as e:
                self.logger.error("Error : {} in adding ".format(str(e),key))

        self.logger.info("Exiting insert_default_users")

if __name__ == '__main__':
    ld = LdapSetup()
    ld.install()
    # ld.insert_default_users()