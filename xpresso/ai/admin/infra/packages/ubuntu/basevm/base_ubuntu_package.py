"""Abstract base class for packages object"""

__all__ = ['BaseUbuntuPackage']
__author__ = 'Naveen Sinha'

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    CommandExecutionFailedException
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    PackageFailedException
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class BaseUbuntuPackage(AbstractPackage):
    """
    Installs Common Packages in the Debian VM
    """

    BASE_UBUNTU_SECTION = "base_ubuntu"
    PKG_LIST_KEY = "pkg_list"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def status(self, **kwargs):
        """
        Checks status of the installed package
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        status_command = "dpkg -s {}".format(
            ' '.join(self.config[self.BASE_UBUNTU_SECTION][self.PKG_LIST_KEY])
        )
        try:
            (code, _, _) = self.executor.execute_with_output(status_command)
            # Check all packages are present
            if code == 0:
                return True
        except CommandExecutionFailedException:
            self.logger.error("Command failed {}".format(status_command))
            raise PackageFailedException(
                "Base Ubuntu Package Installation Failed")
        return False

    def install(self, **kwargs):
        """
        Install docker client into a ubuntu VM
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        self.execute_command("apt-get -y update && apt-get -y upgrade")
        self.execute_command("apt-get -y install {}".format(
            ' '.join(self.config[self.BASE_UBUNTU_SECTION][self.PKG_LIST_KEY])
        ))
        return True

    def uninstall(self, **kwargs):
        """
        Remove packages from ubuntu list
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        self.execute_command("apt-get -y remove {}".format(
            ' '.join(self.config[self.BASE_UBUNTU_SECTION][self.PKG_LIST_KEY])
        ))
        self.execute_command("apt-get -y clean")
        self.execute_command("apt-get -y autoremove")
        self.execute_command("apt-get -y purge")
        return True

    def start(self, **kwargs):
        return True

    def stop(self, **kwargs):
        return True
