"""
Xpresso Controller cli to process argument parser
"""

__all__ = ['XpressoControllerCLI']
__author__ = 'Sahil Malav'

import json
from getpass import getpass
import click

from xpresso.ai.admin.controller.client.cli_response_formatter import \
    CLIResponseFormatter
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    CLICommandFailedException, ControllerClientResponseException
from xpresso.ai.admin.controller.client.controller_client \
    import ControllerClient
from xpresso.ai.core.logging.xpr_log import XprLogger


class XpressoControllerCLI:
    """
    It takes the command line arguments and processes it as needed. xprctl
    binary uses this class to serve the command
    """

    COMMAND_KEY_WORD = "command"
    USER_ARGUMENT = "user"
    FILE_ARGUMENT = "file"
    INPUT_ARGUMENT = "input"
    ALL_ARGUMENT = "all"
    BRANCH_ARGUMENT = "branch"

    def __init__(self):
        self.controller_client = ControllerClient()
        self.command = None
        self.arguments = {}
        self.SUPPORTED_COMMANDS = {}
        self.initialize_commands()
        self.logger = XprLogger()

    def initialize_commands(self):
        """
        Creates a mapping ot command to functions
        """
        try:
            self.SUPPORTED_COMMANDS = {
                "list": self.list_supported_commands,

                "login": self.login,
                "logout": self.logout,

                "get_cluster": self.get_clusters,
                "register_cluster": self.register_cluster,
                "deactivate_cluster": self.deactivate_cluster,

                "get_users": self.get_users,
                "register_user": self.register_user,
                "modify_user": self.modify_user,
                "deactivate_user": self.deactivate_user,
                "update_password": self.update_password,

                "get_nodes": self.get_nodes,
                "register_node": self.register_node,
                "deactivate_node": self.deactivate_node,
                "provision_node": self.provision_node,
                "assign_node": self.assign_node,

                "create_project": self.create_project,
                "register_project": self.create_project,
                "get_project": self.get_project,
                "deactivate_project": self.deactivate_project,
                "build_project": self.build_project,
                "get_build_version": self.get_build_version,
                "deploy_project": self.deploy_project,
                "undeploy_project": self.undeploy_project,
                "modify_project": self.modify_project,

                "version": self.get_version,
                "update": self.update,

                "create_repo": self.create_repo,
                "create_branch": self.create_branch,
                "push_dataset": self.push_dataset,
                "pull_dataset": self.pull_dataset,
                "list_dataset": self.list_dataset
            }
        except AttributeError:
            raise CLICommandFailedException("CLI issue. Contact developer to "
                                            "fix it.")

    def extract_argument(self, argument):
        if argument in self.arguments:
            return self.arguments[argument]
        return None

    def extract_json_from_file_or_input(self):
        """
        Extracts json data from either file or input
        """
        file_fs = self.extract_argument(self.FILE_ARGUMENT)
        input_json = self.extract_argument(self.INPUT_ARGUMENT)
        if input_json:
            try:
                data = json.loads(input_json)
            except json.JSONDecodeError:
                raise CLICommandFailedException(
                    "Invalid Json file")
        elif file_fs:
            try:
                data = json.load(file_fs)
            except json.JSONDecodeError:
                raise CLICommandFailedException(
                    "Invalid Json file")
        else:
            raise CLICommandFailedException(
                "Please provide input json using "
                "-f/--file or -i/--input")
        return data

    def execute(self, **kwargs):
        """
        Validates the command provided and calls the relevant function for
        execution

        Args:
            kwargs: It takes kwargs as argument which should contain the
                    argument passed in command line
        """
        self.arguments = kwargs
        if self.COMMAND_KEY_WORD not in self.arguments:
            raise CLICommandFailedException("No valid command provided."
                                            "Please type xprctl list for "
                                            "complete list of commands")

        command = self.arguments[self.COMMAND_KEY_WORD]

        if command not in self.SUPPORTED_COMMANDS:
            raise CLICommandFailedException(f"{command} not supported")

        try:
            self.logger.info(f"executing command {command}"
                             f"with argument {self.arguments}")
            response = self.SUPPORTED_COMMANDS[command]()
            self.logger.info(f"Command executed with response {response}")
            return response
        except TypeError as e:
            self.logger.error(e)
            raise CLICommandFailedException(f"{command} is not executable")

    def list_supported_commands(self):
        return {"Commands": list(self.SUPPORTED_COMMANDS.keys())}

    def login(self):
        username = self.extract_argument(self.USER_ARGUMENT)
        if not username:
            username = input('Username: ')
        password = getpass()
        return self.controller_client.login(username, password)

    def sso_login(self):
        response = self.controller_client.sso_login()
        if "url" in response:
            message = f"Login here: {response['url']}"
            print(CLIResponseFormatter(data=message).get_str())
            print("Waiting for login to get successful...")
            return self.controller_client.sso_validate(response["validation_token"])
        return response

    def logout(self):
        return self.controller_client.logout()

    def get_clusters(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.get_clusters(data)

    def deactivate_cluster(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.deactivate_cluster(data)

    def register_cluster(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.register_cluster(data)

    def get_users(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.get_users(data)

    def register_user(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.register_user(data)

    def modify_user(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.modify_user(data)

    def deactivate_user(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.deactivate_user(data)

    def register_node(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.register_node(data)

    def get_nodes(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.get_nodes(data)

    def provision_node(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.provision_node(data)

    def deactivate_node(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.deactivate_node(data)

    def assign_node(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.assign_node(data)

    def create_project(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.create_project(data)

    def get_project(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.get_project(data)

    def deactivate_project(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.deactivate_project(data)

    def modify_project(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.modify_project(data)

    def build_project(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.build_project(data)

    def get_build_version(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.get_build_version(data)

    def deploy_project(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.deploy_project(data)

    def undeploy_project(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.undeploy_project(data)

    def get_version(self):
        return self.controller_client.fetch_version()

    def update(self):
        return self.controller_client.update_xpresso()

    def update_password(self):
        username = self.extract_argument(self.USER_ARGUMENT)
        if not username:
            username = input('Username: ')
        old_password = getpass("Current Password:")
        new_password = getpass("New Password:")
        return self.controller_client.update_password(
            {
                "uid": username,
                "old_pwd": old_password,
                "new_pwd": new_password
            }
        )

    def create_repo(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.create_repo(data)

    def create_branch(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.create_branch(data)

    def push_dataset(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.push_dataset(data)

    def pull_dataset(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.pull_dataset(data)

    def list_dataset(self):
        data = self.extract_json_from_file_or_input()
        return self.controller_client.list_dataset(data)


@click.command()
@click.argument('command')
@click.option('-f', '--file', type=click.File(),
              help='Path of the file you want to use as input')
@click.option('-u', '--user', type=str, help='Username')
@click.option('-i', '--input', type=str,
              help='Quick Input. Type a json stub to pass it as input. '
                   'Removes the need to use -f.')
@click.option('--all', type=bool, required=False,
              help='Check if command needs to apply to all')
@click.option('--branch', type=str, required=False,
              help='Provide the branch name')
def cli_options(**kwargs):
    logger = XprLogger()
    xprctl = XpressoControllerCLI()
    try:
        response = xprctl.execute(**kwargs)
        if response:
            click.echo(CLIResponseFormatter(data=response).get_str())
        click.secho("Success", fg="green")
    except (ControllerClientResponseException,
            CLICommandFailedException) as cli_error:
        click.secho(f"Error: {cli_error.message}", err=True, fg="red")
    except Exception as e:
        logger.error(e)
        click.secho(f"Unknown Failure", err=True, fg="red")


if __name__ == "__main__":
    cli_options()
