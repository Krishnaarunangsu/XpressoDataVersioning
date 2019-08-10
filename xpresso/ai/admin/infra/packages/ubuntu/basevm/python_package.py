"""Abstract base class for packages object"""

__all__ = ['PythonPackage']
__author__ = 'Naveen Sinha'

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class PythonPackage(AbstractPackage):
    """
    Installs Python 3.7 package
    """

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def status(self, **kwargs):
        """
        Checks whether correct version of python is installed or not
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        p2_command = "python2 --version"
        p3_command = "python3 --version"
        (_, p2_v, _) = self.execute_command_with_output(p2_command)
        (_, p3_v, _) = self.execute_command_with_output(p3_command)

        if "2.7" in p2_v.decode() and "3.7" in p3_v.decode():
            return True
        return False

    def install(self, **kwargs):
        """
        Installs correct version of python in the vm
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        self.execute_command("apt-get -y install python2")
        self.execute_command("add-apt-repository -y ppa:ubuntu-toolchain-r/ppa")
        self.execute_command("apt-get -y install python3.7")
        self.execute_command("update-alternatives "
                             "--install /usr/bin/python3 python3 "
                             "/usr/bin/python3.6 1")
        self.execute_command("update-alternatives "
                             "--install /usr/bin/python3 python3 "
                             "/usr/bin/python3.7 2")
        self.execute_command("update-alternatives "
                             "--install /usr/bin/python python "
                             "/usr/bin/python3.7 2")
        self.execute_command("cp /usr/lib/python3/dist-packages/"
                             "apt_pkg.cpython-36m-x86_64-linux-gnu.so "
                             "/usr/lib/python3/dist-packages/apt_pkg.so")
        return True

    def uninstall(self, **kwargs):
        """
        Remove python packages
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        self.execute_command("apt-get -y remove python3.7")
        return True

    def start(self, **kwargs):
        return True

    def stop(self, **kwargs):
        return True
