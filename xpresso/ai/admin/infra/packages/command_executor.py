""" Abstract CommandExecutor class which is used to run script on a server"""

__all__ = ["CommandExecutor"]
__author__ = "Naveen Sinha"

from abc import abstractmethod


class CommandExecutor:
    """ It is used to execute any command or script in an environment.
    It interacts with the environment to run the scripts"""

    @abstractmethod
    def execute(self, command: str, **kwargs):
        """ Executes a specific command """

    @abstractmethod
    def execute_with_output(self, command: str, **kwargs):
        """ Executes a specific command and returns the output. Return
        structure may vary depending on the implementation"""

    @abstractmethod
    def execute_same_shell(self, command: str, **kwargs):
        """ Executes a specific command in the same shell environment as the
        program. It will not create separate shell command
        """
