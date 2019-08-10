"""Abstract base class for packages object"""

import time
import docker

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import\
    PackageFailedException

__all__ = ['KongGatewayPackage']
__author__ = 'Naveen Sinha'


class KongGatewayPackage(AbstractPackage):
    """
    Installs Kong service using its docker images. It uses
    cassandra as its database
    """

    DOCKER_PREFIX = "xpresso-kong"
    NETWORK_NAME = DOCKER_PREFIX + "-net"
    DATABASE_NAME = DOCKER_PREFIX + "-database"
    SERVICE_NAME = DOCKER_PREFIX + "-service"
    VOLUME_NAME = DOCKER_PREFIX + "-volume"

    CONFIG_KONG_KEY = "kong"
    CONFIG_MOUNT_PATH = "host_mount_path"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def setup_kong_db(self):
        """ Create database docker for kong """
        self.logger.info("Creating database docker for kong")
        try:
            docker_client = docker.from_env()
            mount_path = self.config[self.CONFIG_KONG_KEY][
                self.CONFIG_MOUNT_PATH]
            docker_client.containers.run(
                image="cassandra:3",
                name=self.DATABASE_NAME,
                detach=True, network=self.NETWORK_NAME,
                volumes={mount_path: {'bind': '/var/lib/cassandra',
                                      'mode': 'rw'}},
                ports={"9042/tcp": "9042"})
            self.logger.info(
                "Cassandra docker is booting up. Waiting 60s for warm up")
            time.sleep(60)
            self.logger.info("Migrating database")
            docker_client.containers.run(
                image="kong:1.2.1", remove=True,
                network=self.NETWORK_NAME,
                environment=["KONG_DATABASE=cassandra",
                             f"KONG_PG_HOST={self.DATABASE_NAME}",
                             f"KONG_CASSANDRA_CONTACT_POINTS={self.DATABASE_NAME}"],
                command=["kong", "migrations",
                         "bootstrap"])
            self.logger.info("Migration completed")
        except (docker.errors.APIError, docker.errors.NotFound):
            error_msg = "Database Creation Failed"
            self.logger.error(error_msg)
            raise PackageFailedException(error_msg)
        self.logger.info("Database docker for kong created")

    def setup_kong_docker_network(self):
        """ Create network for Kong """
        self.logger.info("Creating  network for kong")
        try:
            docker_client = docker.from_env()
            docker_client.networks.create(self.NETWORK_NAME)
        except docker.errors.APIError:
            self.logger.error("Network Creation Failed")
            raise PackageFailedException(f"Docker Network Creation Failed")
        self.logger.info("Network Creation completed")

    def setup_kong(self):
        """ Start Kong service which provide Admin and Proxy API """

        self.logger.info("Creating kong docker image")
        try:
            docker_client = docker.from_env()
            docker_client.containers.run(
                image="kong:1.2.1", detach=True,
                name=self.SERVICE_NAME,
                network=self.NETWORK_NAME,
                ports={"8000/tcp": "8000",
                       "8443/tcp": "8443",
                       "8001/tcp": "8001",
                       "8444/tcp": "8444"},
                environment=["KONG_DATABASE=cassandra",
                             f"KONG_PG_HOST={self.DATABASE_NAME}",
                             f"KONG_CASSANDRA_CONTACT_POINTS={self.DATABASE_NAME}",
                             "KONG_PROXY_ACCESS_LOG=/dev/stdout",
                             "KONG_ADMIN_ACCESS_LOG=/dev/stdout",
                             "KONG_PROXY_ERROR_LOG=/dev/stderr",
                             "KONG_ADMIN_ERROR_LOG=/dev/stderr",
                             "KONG_ADMIN_LISTEN=0.0.0.0:8001, 0.0.0.0:8444 ssl"]
            )
        except (docker.errors.NotFound, docker.errors.APIError):
            error_msg = "Kong service creation failed"
            self.logger.error(error_msg)
            raise PackageFailedException(error_msg)

        self.logger.info("Kong service has been created")

    def status(self, **kwargs):
        """
        Checks the status of existing running application

        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        self.logger.info("Checking current status of docker")
        try:
            docker_client = docker.from_env()
            network_list = docker_client.networks.list(
                names=[self.NETWORK_NAME])
            # No network exist
            if not network_list:
                return False
            _ = docker_client.containers.get(self.DATABASE_NAME)
            _ = docker_client.containers.get(self.SERVICE_NAME)
        except(docker.errors.NotFound, docker.errors.APIError):
            self.logger.error("Status check failed")
            return False
        self.logger.info("Status check succeeded")
        return True

    def install(self, **kwargs):
        """
        Install Kong gateway Service using docker
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """

        if self.status():
            self.logger.info("Kong Already installed")

        self.logger.info("Clear everything and then install")
        self.uninstall()
        self.logger.info("Clear successfull. Now installing kong")
        self.setup_kong_docker_network()
        self.setup_kong_db()
        self.setup_kong()
        self.logger.info("Kong has been installed")
        return True

    def remove_docker_container(self, docker_client, docker_name):
        """ Check if container exists. If yes, then remove it"""
        self.logger.debug(f"Removing container {docker_name}")
        try:
            database_docker = docker_client.containers.get(docker_name)
            database_docker.stop()
            database_docker.remove(force=True)
            self.logger.debug(f"Container removed: {docker_name}")
        except docker.errors.NotFound:
            self.logger.debug(f"Container does not exist: {docker_name}")

    def start_docker_container(self, docker_client, docker_name):
        """ Check if container exists. If yes, then start it"""
        self.logger.debug(f"Starting container {docker_name}")
        try:
            database_docker = docker_client.containers.get(docker_name)
            database_docker.start()
            self.logger.debug(f"Container started: {docker_name}")
        except docker.errors.NotFound:
            self.logger.debug(f"Container does not exist: {docker_name}")

    def stop_docker_container(self, docker_client, docker_name):
        """ Check if container exists. If yes, then stop it"""
        self.logger.debug(f"Stopping container {docker_name}")
        try:
            database_docker = docker_client.containers.get(docker_name)
            database_docker.stop()
            self.logger.debug(f"Container stopped: {docker_name}")
        except docker.errors.NotFound:
            self.logger.debug(f"Container does not exist: {docker_name}")

    def uninstall(self, **kwargs):
        """
        Removes the docker container and network being used
        for Kong
        Returns:
            True, if uninstall is successful. False Otherwise
        Raises:
            PackageFailedException
        """

        self.logger.info("Checking self check failed")
        try:
            docker_client = docker.from_env()

            self.remove_docker_container(docker_client, self.SERVICE_NAME)
            self.remove_docker_container(docker_client, self.DATABASE_NAME)

            network_list = docker_client.networks.list(
                names=[self.NETWORK_NAME])
            if network_list:
                network = network_list[0]
                network.remove()

        except docker.errors.APIError:
            self.logger.error("Status check failed")
            return False
        return True

    def start(self, **kwargs):
        """ Start docker service for Kong """
        try:
            docker_client = docker.from_env()
            self.start_docker_container(docker_client, self.DATABASE_NAME)
            self.start_docker_container(docker_client, self.SERVICE_NAME)
        except(docker.errors.NotFound, docker.errors.APIError):
            self.logger.error("Docker start failed")
            return False
        return True

    def stop(self, **kwargs):
        """ Stop the docker service for Kong """
        try:
            docker_client = docker.from_env()
            self.stop_docker_container(docker_client, self.SERVICE_NAME)
            self.stop_docker_container(docker_client, self.DATABASE_NAME)
        except(docker.errors.NotFound, docker.errors.APIError):
            self.logger.error("Docker stop failed")
            return False
        return True


if __name__ == "__main__":
    kong_package = KongGatewayPackage(config_path="config/common.json")
    kong_package.install()
