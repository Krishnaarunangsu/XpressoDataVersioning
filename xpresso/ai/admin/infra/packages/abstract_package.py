"""Abstract base class for packages object"""
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser

__all__ = ['AbstractPackage']
__author__ = 'Naveen Sinha'

from abc import abstractmethod

from xpresso.ai.admin.infra.packages.command_executor import CommandExecutor
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    CommandExecutionFailedException
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    PackageFailedException
from xpresso.ai.core.logging.xpr_log import XprLogger


class AbstractPackage:
    """
    Abstract base class for packages. It has one job to do, is execute one
    packages
    """

    def __init__(self, executor: CommandExecutor = None,
                 config_path: XprConfigParser =
                 XprConfigParser.DEFAULT_CONFIG_PATH):
        self.executor = executor
        self.config_path = config_path
        self.logger = XprLogger()

    @abstractmethod
    def install(self, **kwargs):
        """
        Run installation scripts
        """

    @abstractmethod
    def uninstall(self, **kwargs):
        """
        Removes installed libraries
        """

    @abstractmethod
    def start(self, **kwargs):
        """
        Start the service/stop if required
        """

    @abstractmethod
    def stop(self, **kwargs):
        """
        Stop the service/stop if required
        """

    @abstractmethod
    def status(self, **kwargs):
        """
        Checks is the libraries are installed and running
        Returns:
             bool: True, if libraries are setup correctly
        """

    def execute_command(self, command):
        self.logger.info(f"Running command: {command}")
        try:
            return self.executor.execute(command)
        except CommandExecutionFailedException:
            self.logger.error("Command failed {}".format(command))
            raise PackageFailedException(
                "Base Ubuntu Package Installation Failed")

    def execute_command_with_output(self, command):
        self.logger.info(f"Running command: {command}")
        try:
            return self.executor.execute_with_output(command)
        except CommandExecutionFailedException:
            self.logger.error("Command failed {}".format(command))
            raise PackageFailedException(
                "Base Ubuntu Package Installation Failed")
