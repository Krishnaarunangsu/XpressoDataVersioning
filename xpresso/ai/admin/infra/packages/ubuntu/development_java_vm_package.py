"""Abstract base class for packages object"""


__all__ = ['DevelopmentJavaVMPackage']
__author__ = 'Naveen Sinha'

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class DevelopmentJavaVMPackage(AbstractPackage):
    """
    Installs Common Packages in the Debian VM
    """

    CONFIG_SECTION = "development_vm"
    REQUIREMENT_KEY = "requirement_file"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def status(self, **kwargs):
        """
        Checks the status of packages in the VM
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        return True

    def install(self, **kwargs):
        """
        Installs all packages for development VM
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        # Setting up Java Library
        self.logger.info("Setting up Java")
        self.execute_command('echo "deb http://ppa.launchpad.net/webupd8team/'
                             'java/ubuntu trusty main" >> '
                             '/etc/apt/sources.list.d/java-8-debian.list')
        self.execute_command('echo "deb-src http://ppa.launchpad.net/'
                             'webupd8team/java/ubuntu trusty main" >> '
                             '/etc/apt/sources.list.d/java-8-debian.list')
        self.execute_command("apt-key adv --keyserver keyserver.ubuntu.com "
                             "--recv-keys EEA14886")
        self.execute_command("apt-get update")
        self.execute_command("echo debconf shared/accepted-oracle-license-v1-1 "
                             "select true | debconf-set-selections")
        self.execute_command("echo debconf shared/accepted-oracle-license-v1-1 "
                             "seen true | debconf-set-selections")
        self.execute_command("ACCEPT_EULA=Y apt-get install -y "
                             "oracle-java8-installer")
        self.logger.info("Installing Maven")
        self.execute_command("apt-get install -y maven")

        # Installing eclipse
        self.execute_command("snap install --classic eclipse")
        return True

    def uninstall(self, **kwargs):
        """
        Removes all extra packages installed for development VM
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        return True

    def start(self, **kwargs):
        return True

    def stop(self, **kwargs):
        return True
