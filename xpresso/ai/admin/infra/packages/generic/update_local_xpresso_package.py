"""Abstract base class for packages object"""

__all__ = ['UpdateLocalXpressoPackage']
__author__ = 'Naveen Sinha'

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class UpdateLocalXpressoPackage(AbstractPackage):
    """
    Installs Common Packages in the Debian VM
    """

    DEFAULT_PROJECT_BRANCH = "master"
    PARAMETER_BRANCH_NAME = "branch_name"
    PARAMETER_KEY = "parameters"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)
        self.default_project_path = self.config["general"]["package_path"]

    def get_current_branch(self):
        rc, stdout, stderr = self.execute_command_with_output(
            command="git branch 2> /dev/null | "
                    "sed -e '/^[^*]/d' -e 's/* \(.*\)/\\1/'")
        if rc == 0:
            return stdout.decode().strip()
        return self.DEFAULT_PROJECT_BRANCH

    def get_branch_from_parameter(self, **kwargs):
        required_branch = self.get_current_branch()
        self.logger.info(f"Branch {required_branch}")
        if (self.PARAMETER_KEY in kwargs and kwargs[self.PARAMETER_KEY] and
            self.PARAMETER_BRANCH_NAME in kwargs[self.PARAMETER_KEY]):
            required_branch = \
                kwargs[self.PARAMETER_KEY][self.PARAMETER_BRANCH_NAME]

        return required_branch

    def fetch_and_checkout(self, branch_name):
        self.execute_command("git fetch")
        self.execute_command(f"git checkout -f {branch_name}")

    def status(self, **kwargs):
        """
        Checks if current xpresso project is up to date
        Returns:
            True/False
        Raises:
            PackageFailedException
        """
        branch_name = self.get_branch_from_parameter(**kwargs)
        # self.fetch_and_checkout(branch_name)
        # Get the last local and remote commit and compare
        return_code, stdout, stderr = self.execute_command_with_output(
            f"git rev-parse {branch_name} origin/{branch_name}"
        )
        commit_list = stdout.decode().split("\n")
        # if last local and last remote commit matches, then it means there is
        # no update
        if len(commit_list) >= 2 and commit_list[0] == commit_list[1]:
            return True
        return False

    def install(self, **kwargs):
        """
        Update the current xpresso project using git. Rebase and pull latest
        update from the git repository

        Args:
            **kwargs:
        Returns:
            True, if setup is successful. False Otherwise
        Raises:
            PackageFailedException
        """
        branch_name = self.get_branch_from_parameter(**kwargs)
        self.fetch_and_checkout(branch_name)
        self.execute_command(f"git fetch && git reset --hard")
        self.execute_command("pip3 install .")
        return self.status(**kwargs)

    def uninstall(self, **kwargs):
        """
        Nothing is here to uninstall

        Args:
            **kwargs:

        Returns:
            True, if setup is successful. False Otherwise
        """
        return True

    def start(self, **kwargs):
        return True

    def stop(self, **kwargs):
        return True


if __name__ == "__main__":
    update_local_package = UpdateLocalXpressoPackage()
    update_local_package.install()
