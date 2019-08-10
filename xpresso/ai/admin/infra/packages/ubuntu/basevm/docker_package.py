"""Abstract base class for packages object"""

__all__ = ['DockerPackage']
__author__ = 'Naveen Sinha'

import os

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class DockerPackage(AbstractPackage):
    """
    Installs Docker client and server in an ubuntu VM
    """

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def status(self, **kwargs):
        (code, output, err) = self.execute_command_with_output(
            "systemctl show --property ActiveState docker")
        if "inactive" in str(output).split('=')[-1]:
            return True
        return False

    def install(self, **kwargs):
        """
        Install docker client into a ubuntu VM
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        # Install Docker
        self.execute_command(
            "curl -fsSL https://download.docker.com/linux/ubuntu/gpg "
            "| apt-key add -")
        self.execute_command(
            'add-apt-repository "deb [arch=amd64] '
            'https://download.docker.com/linux/ubuntu bionic stable"')
        self.execute_command("apt-get -y update")
        self.execute_command("apt-cache policy docker-ce")
        self.execute_command("apt-get install -y docker-ce")

        # Install Docker Compos
        self.execute_command(
            'curl -L "https://github.com/docker/compose/releases/download/'
            '1.23.1/docker-compose-$(uname -s)-$(uname -m)" '
            '-o /usr/local/bin/docker-compose')
        self.execute_command('chmod +x /usr/local/bin/docker-compose')
        return True

    def uninstall(self, **kwargs):
        self.execute_command("apt-get -y remove docker-ce")
        try:
            os.remove('/usr/local/bin/docker-compose')
        except OSError:
            # Not raising it again as we don't care about it
            self.logger.warning("Docker Compose not found")
        return True

    def start(self, **kwargs):
        self.execute_command("systemctl start docker")
        return True

    def stop(self, **kwargs):
        self.execute_command("systemctl stop docker")
        return True


if __name__ == "__main__":
    docker_cmd = DockerPackage()
