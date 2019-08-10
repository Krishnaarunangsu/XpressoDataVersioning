""" Abstract CommandExecutor class which is used to run script on a server"""

__all__ = ["LocalShellExecutor"]
__author__ = "Naveen Sinha"

import subprocess

from xpresso.ai.admin.infra.packages.command_executor import CommandExecutor
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import CommandExecutionFailedException
from xpresso.ai.core.logging.xpr_log import XprLogger


class LocalShellExecutor(CommandExecutor):
    """ It is used to run shell commands locally on a linux environment"""

    DEFAULT_EXECUTOR = "/bin/bash"

    def __init__(self):
        super().__init__()
        self.logger = XprLogger()

    def execute_with_output(self, command: str, executor=DEFAULT_EXECUTOR):
        """ It runs linux shell command on local server and returns the
        output

        Args;
            command(str): command for execution

        Returns:
            tuple: (response code: int, stdout: str, stderr: str)
        """
        self.logger.debug("Running command {}".format(command))
        try:
            status = subprocess.run(command, capture_output=True, shell=True,
                                    executable=executor)
            self.logger.debug("Command successful")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            self.logger.warning("Command failed")
            raise CommandExecutionFailedException(str(e))
        return status.returncode, status.stdout, status.stderr

    def execute(self, command: str, executor=DEFAULT_EXECUTOR):
        """ It runs linux shell command on local server

        Args;
            command(str): command for execution
        Returns:
            int: response code
        """
        self.logger.debug("Running command {}".format(command))
        try:
            status = subprocess.run(command, shell=True, executable=executor)
            self.logger.debug("Command successful")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            self.logger.warning("Command failed")
            raise CommandExecutionFailedException(str(e))
        return status.returncode

    def execute_same_shell(self, command: str, executor=DEFAULT_EXECUTOR):
        """ It runs linux shell command on local server and returns the
        output

        Args;
            command(str): command for execution

        Returns:
            tuple: (response code: int, stdout: str, stderr: str)
        """
        self.logger.debug("Running command {}".format(command))
        try:
            status = subprocess.run(command, capture_output=True, shell=False,
                                    executable=executor)
            self.logger.debug("Command successful")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            self.logger.warning("Command failed")
            raise CommandExecutionFailedException(str(e))
        return status.returncode, status.stdout, status.stderr
