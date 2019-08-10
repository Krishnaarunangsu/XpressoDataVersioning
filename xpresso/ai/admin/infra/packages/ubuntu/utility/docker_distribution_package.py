"""Abstract base class for packages object"""

__all__ = ['DockerDistributionPackage']
__author__ = 'Naveen Sinha'

import os
import shutil

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import\
    PackageFailedException


class DockerDistributionPackage(AbstractPackage):
    """
    Installs Docker Distribution Services. It installs open source Harbor
    project to manage the docker registry. This installs the Harbor project
    only.
    """

    CONFIG_SECTION = "docker_distribution"
    HARBOR_CFG_FILE = "harbor_cfg_file"
    HARBOR_COMPOSE_FILE = "harbor_compose_file"
    HARBOR_TMP_FOLDER = "harbor_folder"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def status(self, **kwargs):
        """
        Checks the status of existing running application

        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        docker_name_list = ["nginx", "harbor-portal", "harbor-jobservice",
                            "harbor-core", "harbor-adminserver", "", "registry",
                            "registryctl", "harbor-persistence", "redis", "harbor-log"]
        (_, output, _) = self.execute_command_with_output(
            "docker inspect -f '{{.State.Running}}' {}".format(
                ' '.join(docker_name_list)
            )
        )
        if 'false' in output:
            return False
        return True

    def install(self, **kwargs):
        """
        Sets up docker distribution in a VM
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        current_directory = os.getcwd()
        harbor_folder = self.config[self.CONFIG_SECTION][self.HARBOR_TMP_FOLDER]
        try:
            if not os.path.exists(harbor_folder):
                os.makedirs(harbor_folder)
        except OSError:
            self.logger.error("Can not create directory")
            raise PackageFailedException("Harbor temp folder can't be created")

        self.execute_command(
            "wget https://storage.googleapis.com/harbor-releases/"
            "release-1.7.0/harbor-online-installer-v1.7.5.tgz -O "
            "{}/harbor.tgz".format(harbor_folder))
        os.chdir(harbor_folder)
        self.execute_command("tar xvf harbor.tgz".format())
        extracted_folder = os.path.join(harbor_folder, "harbor")
        try:
            os.chdir(extracted_folder)
        except OSError:
            self.logger.error("Harbor Folder not found")
            raise PackageFailedException("Harbor Folder not found")

        os.chdir(current_directory)
        shutil.copy(self.config[self.CONFIG_SECTION][self.HARBOR_CFG_FILE],
                    extracted_folder)
        shutil.copy(self.config[self.CONFIG_SECTION][self.HARBOR_COMPOSE_FILE],
                    extracted_folder)

        os.chdir(extracted_folder)
        self.execute_command("/bin/bash install.sh")
        os.chdir(current_directory)
        return True

    def uninstall(self, **kwargs):
        """
        Remove docker distribution
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """

        """
        cd $PWD/config/harbor
        docker-compose up -d
        """
        harbor_tmp_dir = self.config[self.CONFIG_SECTION][
            self.HARBOR_TMP_FOLDER]
        harbor_dir = os.path.join(harbor_tmp_dir, "harbor")

        try:
            os.chdir(harbor_dir)
        except OSError:
            self.logger("{} not found.".format(harbor_dir))
            raise PackageFailedException(
                "{} not found. Required for stopping".format(harbor_dir))
        self.execute_command("/usr/local/bin/docker-compose up -d")
        return True

    def start(self, **kwargs):
        return self.install()

    def stop(self, **kwargs):
        return self.uninstall()
