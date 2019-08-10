"""Abstract base class for packages object"""


__all__ = ['DevelopmentPythonVMPackage']
__author__ = 'Naveen Sinha'


from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class DevelopmentPythonVMPackage(AbstractPackage):
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
        snap_list_check = "snap list | grep pycharm"
        (_, output, _) = self.execute_command_with_output(snap_list_check)
        if "pycharm" not in output:
            return False

        snap_list_check = "pip3 freeze | cut -f1 -d="
        (_, output, _) = self.execute_command_with_output(snap_list_check)
        output_list = set(output.splitlines())
        req_file = self.config[self.CONFIG_SECTION][self.REQUIREMENT_KEY]
        req_list_check = "cat {}| cut -f1 -d=".format(req_file)
        (_, output, _) = self.execute_command_with_output(req_list_check )
        req_list = set(output.splitlines())
        if not req_list.issubset(output_list):
            return False
        return True

    def install(self, **kwargs):
        """
        Installs all packages for development VM
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """

        # Setup Python Packages
        self.logger.info("Setting up base python packages")
        req_file = self.config[self.CONFIG_SECTION][self.REQUIREMENT_KEY]
        self.execute_command("pip3 install -r {}".format(req_file))
        self.execute_command("python3 -m spacy download en_core_web_sm")
        self.execute_command("python3 -m spacy download en")
        self.execute_command("python3 -m spacy download en_core_web_md")
        self.execute_command("python3 -c \"import nltk;nltk.download('punkt');"
                             "nltk.download('averaged_perceptron_tagger');"
                             "nltk.download('wordnet')\"")
        self.logger.info("Python packages setup done")

        # Setup Pycharm
        self.logger.info("Setting up IDE Pycharm")
        self.execute_command("apt-get -y install snapd snapd-xdg-open")
        self.execute_command("snap install pycharm-community --classic")
        return True

    def uninstall(self, **kwargs):
        """
        Removes all extra packages installed for development VM
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        self.logger.info("Uninstall base packages")
        req_file = self.config[self.CONFIG_SECTION][self.REQUIREMENT_KEY]
        self.execute_command("pip3 uninstall -y -r {}".format(req_file))
        self.logger.info("All python packages uninstalled")
        # Removing Dev IDE
        self.execute_command("snap remove pycharm-community")
        return True

    def start(self, **kwargs):
        return True

    def stop(self, **kwargs):
        return True
