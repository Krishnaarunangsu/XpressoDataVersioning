from json import JSONDecodeError

__all__ = ['ControllerClient']
__author__ = 'Sahil Malav'

import os
from json import JSONDecodeError
import time
import json

from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.utils import error_codes
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.generic_utils import get_version
from xpresso.ai.admin.controller.network.http.http_request import HTTPRequest, \
    HTTPMethod
from xpresso.ai.admin.controller.network.http.http_request_handler import \
    HTTPHandler
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    HTTPRequestFailedException
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    HTTPInvalidRequestException
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    ControllerClientResponseException
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    FileNotFoundException
from xpresso.ai.admin.infra.packages.package_manager import ExecutionType
from xpresso.ai.admin.infra.packages.package_manager import PackageManager
import xpresso.ai.admin.controller.pachyderm_repo_management.pachyderm_repo_manager as repo_manager
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import XprExceptions

class ControllerClient:
    CONTROLLER_SECTION = 'controller'
    SERVER_URL = 'server_url'
    CLIENT_PATH = 'client_path'
    JENKINS_SECTION = 'jenkins'
    JENKINS_HOST = 'master_host'
    relogin_response = {
        "outcome": "failure",
        "error_code": "106",
        "results": {}
    }

    API_JSON_OUTCOME = "outcome"
    API_JSON_RESULTS = "results"
    API_JSON_ERROR_CODE = "error_code"
    API_JSON_SUCCESS = "success"
    API_JSON_FAILURE = "failure"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH):
        self.logger = XprLogger()
        self.config = XprConfigParser(config_path)
        self.path = os.path.join(
            os.path.expanduser('~'),
            self.config[self.CONTROLLER_SECTION][self.CLIENT_PATH])
        self.token_file = '{}.current'.format(self.path)
        self.server_path = self.config[self.CONTROLLER_SECTION][self.SERVER_URL]

    def sso_login(self):
        """ It performs Single Sign-On authentication for the client.
        It follows following steps
        1. Check if token exists
        2. If exists: Send to the server for validation
            2.1 If token is validated then login is successful
            2.2 If token is not validated, assume token does not exist and go
            to point 3
        3. If no token exists:
            3.1 Print the SSO authentication url for user to login
            3.2 Send request to server every few seconds to check if user
            signed in successful. Wait for 60 seconds. Throw error if not
            logged in
            3.3 When user logged in, fetch the token and save

        """
        self.logger.info('CLIENT : Entering SSO Login Method')

        # Check if token exists:
        try:
            token = self.get_token()
        except ControllerClientResponseException:
            self.logger.info("No Token found")
            token = None

        # Since no token exist, ask for new login
        if token:
            url = f"{self.server_path}/sso/token_login"
            self.logger.debug('CLIENT : Making post request to server')
            data = {"token": token}
            try:
                response = self.send_http_request(url=url,
                                                  header=data,
                                                  http_method=HTTPMethod.POST,
                                                  data=data)
                return response
            except ControllerClientResponseException as e:
                self.logger.info("Assuming logging request failed")
                self.logger.info(e.message)

        url = f"{self.server_path}/sso/get_authentication_url"
        self.logger.debug('CLIENT : Making post request to server')
        response = self.send_http_request(url=url,
                                          http_method=HTTPMethod.GET)
        return response

    def sso_validate(self, validation_token):
        """
        Check whether SSO authentication is completed and successful
        Args:
            validation_token: sso validation token which is used to check if
                              a user has logged in or not.
        Returns:
        """
        # We keep requesting the sso server to test for
        interval_second = 2
        wait_second = 60
        start_time = time.time()
        while time.time() - start_time < wait_second:
            self.logger.debug('CLIENT : Making post request to server')
            url = f"{self.server_path}/sso/validate"
            data = {"validation_token": validation_token}
            try:

                response = self.send_http_request(url=url,
                                                  http_method=HTTPMethod.POST,
                                                  data=data)
                self.logger.info("Token validated")
                self.save_token(response["token"])
                return {"message": "SSO Login Successfull"}
            except ControllerClientResponseException:
                time.sleep(interval_second)
        self.logger.info('CLIENT : Existing SSO Login Method')
        raise ControllerClientResponseException(
            "Session over without login", error_codes.server_error)

    def login(self, username, password):
        """Sends request to Controller server and
        get the status on login request"""
        self.logger.info('CLIENT : entering login method')
        if not os.path.isdir(self.path):
            os.makedirs(self.path, 0o755)
        if os.path.isfile(self.token_file):
            os.remove(self.token_file)

        if not username:
            self.logger.error('CLIENT : Empty username passed. Exiting.')
            raise ControllerClientResponseException(
                "Username can't be empty", error_codes.empty_uid)
        if not password:
            self.logger.error('CLIENT : Empty password passed. Exiting.')
            raise ControllerClientResponseException(
                "Password can't be empty", error_codes.empty_uid)

        url = f"{self.server_path}/auth"
        credentials = {"uid": username, "pwd": password}
        self.logger.debug('CLIENT : Making post request to server')
        response = self.send_http_request(url=url, http_method=HTTPMethod.POST,
                                          data=credentials)

        self.save_token(token=response['access_token'])
        if 'relogin' in response and response['relogin']:
            self.logger.debug(
                'CLIENT : already logged in. Saving new token.')
            return {"message": f"You are already logged in"}
        elif 'relogin' in response and not response['relogin']:
            self.logger.info(
                'CLIENT : Login successful. Writing token to file.')
            return {"message": f"Welcome, {username}!"}
        return response

    def save_token(self, token):
        """Token is saved in the local file system for """
        file = open(self.token_file, 'w+')
        file.write(token)
        file.close()
        self.logger.info('CLIENT : Token written to file. Exiting.')

    def get_token(self):
        """Token is saved in the local file system for """
        token = None
        try:
            with open(self.token_file, "r") as f:
                token = f.read()
        except FileNotFoundError:
            self.logger.error("No Token Found. Need to Relogin")
            raise ControllerClientResponseException(
                "No Session found. Login again", error_codes.expired_token)
        return token

    def logout(self):
        self.logger.info('CLIENT : entering logout method')
        url = f'{self.server_path}/auth'
        token = self.get_token()
        headers = {'token': token}
        self.logger.debug('CLIENT : Making delete request to server')
        self.send_http_request(url=url,
                               http_method=HTTPMethod.DELETE,
                               header=headers)
        os.remove(self.token_file)
        self.logger.info('CLIENT : Logout successful. Exiting.')
        return {"message": "Successfully logged out"}

    def get_clusters(self, argument):
        self.logger.info(f'CLIENT : entering get_clusters method '
                         f'with arguments {argument}')
        url = f'{self.server_path}/clusters'
        headers = {"token": self.get_token()}
        self.logger.debug('CLIENT : Making get request to server')
        response = self.send_http_request(url=url, http_method=HTTPMethod.GET,
                                          header=headers, data=argument)
        self.logger.info('CLIENT : Get request successful. Exiting.')
        return response

    def deactivate_cluster(self, argument):

        self.logger.info('CLIENT : Entering deactivate_cluster method')
        if not argument:
            self.logger.error('CLIENT : No input arguments provided. Exiting.')
            raise ControllerClientResponseException(
                f"Please provide some input arguments ===",
                error_codes.incomplete_cluster_info)
        url = f'{self.server_path}/clusters'
        headers = {"token": self.get_token()}
        self.send_http_request(url=url, http_method=HTTPMethod.DELETE,
                               header=headers, data=argument)
        self.logger.info('CLIENT : Deactivation successful. Exiting.')
        return {"message": "Cluster deactivated."}

    def register_cluster(self, argument):
        self.logger.info('CLIENT : Entering register_cluster '
                         'with arguments {}'.format(argument))
        if not argument:
            self.logger.error('CLIENT : No input arguments provided. Exiting.')
            raise ControllerClientResponseException(
                f"Please provide some input arguments ===",
                error_codes.incomplete_cluster_info)
        url = f'{self.server_path}/clusters'
        headers = {"token": self.get_token()}
        response = self.send_http_request(url=url, http_method=HTTPMethod.POST,
                                          header=headers, data=argument)
        self.logger.info(
            'CLIENT : Cluster registration successful.Exiting.')
        return {"message": f"Cluster successfully registered with "
        f"ID {response} ###"}

    def register_user(self, user_json):
        url = f"{self.server_path}/users"
        response = self.send_http_request(url=url, http_method=HTTPMethod.POST,
                                          header={"token": self.get_token()},
                                          data=user_json)
        return response

    def get_users(self, filter_json):
        url = f"{self.server_path}/users"
        response = self.send_http_request(url=url, http_method=HTTPMethod.GET,
                                          header={"token": self.get_token()},
                                          data=filter_json)
        return response

    def modify_user(self, changes_json):
        url = f"{self.server_path}/users"
        response = self.send_http_request(url=url, http_method=HTTPMethod.PUT,
                                          header={"token": self.get_token()},
                                          data=changes_json)
        return response

    def update_password(self, password_json):
        url = f"{self.server_path}/user/pwd"
        response = self.send_http_request(url=url, http_method=HTTPMethod.PUT,
                                          header={"token": self.get_token()},
                                          data=password_json)
        return response

    def deactivate_user(self, uid_json):
        url = f"{self.server_path}/users"
        response = self.send_http_request(url=url,
                                          http_method=HTTPMethod.DELETE,
                                          header={"token": self.get_token()},
                                          data=uid_json)
        return response

    def register_node(self, node_json):
        url = f"{self.server_path}/nodes"
        response = self.send_http_request(url=url, http_method=HTTPMethod.POST,
                                          header={"token": self.get_token()},
                                          data=node_json)
        print(response)
        return response

    def get_nodes(self, filter_json):
        url = f"{self.server_path}/nodes"
        response = self.send_http_request(url=url, http_method=HTTPMethod.GET,
                                          header={"token": self.get_token()},
                                          data=filter_json)
        return response

    def provision_node(self, changes_json):
        url = f"{self.server_path}/nodes"
        response = self.send_http_request(url=url, http_method=HTTPMethod.PUT,
                                          header={"token": self.get_token()},
                                          data=changes_json)
        return response

    def deactivate_node(self, node_json):
        url = f"{self.server_path}/nodes"
        response = self.send_http_request(url=url,
                                          http_method=HTTPMethod.DELETE,
                                          header={"token": self.get_token()},
                                          data=node_json)
        return response

    def assign_node(self, assign_json):
        url = f"{self.server_path}/assign_node"
        response = self.send_http_request(url=url, http_method=HTTPMethod.PUT,
                                          header={"token": self.get_token()},
                                          data=assign_json)
        return response

    def check_for_declarative_json(self, project_json):
        """
        Checks if the provided declarative json exists and replaces that field
        with the contents of declarative json.
        Args:
            project_json: input file from user

        Returns: modified project_json

        """
        for pipeline in project_json['pipelines']:
            if not os.path.isfile(pipeline['declarative_json']):
                self.logger.error("declarative json not found")
                raise FileNotFoundException('Declarative JSON not found.')
            with open(pipeline['declarative_json'], 'r') as f:
                declarative_json_data = json.load(f)
                pipeline['declarative_json'] = declarative_json_data
        return project_json

    def create_project(self, project_json):
        if 'pipelines' in project_json:
            project_json = self.check_for_declarative_json(project_json)
        url = f"{self.server_path}/projects/manage"
        response = self.send_http_request(url=url, http_method=HTTPMethod.POST,
                                          header={"token": self.get_token()},
                                          data=project_json)
        return response

    def get_project(self, filter_json):
        url = f"{self.server_path}/projects/manage"
        response = self.send_http_request(url=url, http_method=HTTPMethod.GET,
                                          header={"token": self.get_token()},
                                          data=filter_json)
        return response

    def modify_project(self, changes_json):
        if 'pipelines' in changes_json:
            changes_json = self.check_for_declarative_json(changes_json)
        url = f"{self.server_path}/projects/manage"
        response = self.send_http_request(url=url, http_method=HTTPMethod.PUT,
                                          header={"token": self.get_token()},
                                          data=changes_json)
        return response

    def deactivate_project(self, project_json):
        url = f"{self.server_path}/projects/manage"
        response = self.send_http_request(url=url,
                                          http_method=HTTPMethod.DELETE,
                                          header={"token": self.get_token()},
                                          data=project_json)
        print("response is ", response)
        return response

    def build_project(self, argument):
        self.logger.info(f'CLIENT : Entering build_project '
                         f'with arguments {argument}')
        if not argument:
            self.logger.error('CLIENT : No input arguments provided. Exiting.')
            raise ControllerClientResponseException(
                f"Please provide some input arguments ===",
                error_codes.incomplete_cluster_info)
        url = f'{self.server_path}/projects/build'
        response = self.send_http_request(url=url, http_method=HTTPMethod.POST,
                                          header={"token": self.get_token()},
                                          data=argument)
        self.logger.info('CLIENT : Project build successful.Exiting.')
        return {"message": "Project build successful!",
                "Build IDS": response,
                "Jenkins Pipeline":
                    f"{self.config[self.JENKINS_SECTION][self.JENKINS_HOST]}"
                    f"/blue/pipelines"}

    def get_build_version(self, argument):
        self.logger.info(f'CLIENT : entering get_build_version method '
                         f'with arguments {argument}')
        url = f'{self.server_path}/projects/build'
        self.logger.debug('CLIENT : Making get request to server')
        response = self.send_http_request(url=url, http_method=HTTPMethod.GET,
                                          header={"token": self.get_token()},
                                          data=argument)
        return response

    def deploy_project(self, argument):
        self.logger.info(f'CLIENT : Entering deploy_project '
                         f'with arguments {argument}')
        if not argument:
            self.logger.error('CLIENT : No input arguments provided. Exiting.')

            raise ControllerClientResponseException(
                f"Please provide some input arguments ===",
                error_codes.incomplete_cluster_info)
        url = f'{self.server_path}/projects/deploy'
        response = self.send_http_request(url=url, http_method=HTTPMethod.POST,
                                          header={"token": self.get_token()},
                                          data=argument)
        self.logger.info(
            'CLIENT : Project deployed successfully.Exiting.')
        return {"message": "Project deployed successfully on the below IPs!",
                "Output": response}

    def undeploy_project(self, argument):
        self.logger.info(f'CLIENT : Entering undeploy_project '
                         f'with arguments {argument}')
        if not argument:
            self.logger.error('CLIENT : No input arguments provided. Exiting.')
            raise ControllerClientResponseException(
                f"Please provide some input arguments ===",
                error_codes.incomplete_cluster_info)

        url = f'{self.server_path}/projects/deploy'
        response = self.send_http_request(url=url,
                                          http_method=HTTPMethod.DELETE,
                                          header={"token": self.get_token()},
                                          data=argument)
        self.logger.info(
            'CLIENT : Project undeployed successfully.Exiting.')
        return {"message": "Project undeployed successfully!"}

    def update_xpresso(self):
        """
        Update xpresso project to the latest commit
        """
        # Send request to update server
        server_update_is_success = False
        url = f'{self.server_path}/update_xpresso'
        try:
            self.send_http_request(url, HTTPMethod.POST)
            server_update_is_success = True
        except ControllerClientResponseException as e:
            self.logger.error(e)

        # Update local
        package_manager = PackageManager()
        package_manager.run(package_to_install="UpdateLocalXpressoPackage",
                            execution_type=ExecutionType.INSTALL)
        response = {"client": "Updated"}
        if server_update_is_success:
            response["server"] = "Updated"
        else:
            response["server"] = "Update Failed"
        return response

    def fetch_version(self):
        """
        Fetches server version and client version, convert to a dict and
        returns.
        """
        url = f'{self.server_path}/version'
        json_response = self.send_http_request(url, HTTPMethod.GET)
        server_version = "None"
        if "version" in json_response:
            server_version = json_response["version"]
        client_version = get_version()
        return {
            "client_version": client_version,
            "server_version": server_version
        }

    def send_http_request(self, url: str, http_method: HTTPMethod,
                          data=None, header: dict = None):
        request = HTTPRequest(method=http_method, url=url, headers=header,
                              data=data)
        handler = HTTPHandler()
        try:
            response = handler.send_request(request)
            json_response = response.get_data_as_json()
            if not json_response:
                raise ControllerClientResponseException(
                    "Request Failed", error_codes.server_error)
            elif (json_response[self.API_JSON_OUTCOME] == self.API_JSON_SUCCESS
                  and self.API_JSON_RESULTS in json_response):
                return json_response[self.API_JSON_RESULTS]
            elif (json_response[self.API_JSON_OUTCOME] == self.API_JSON_SUCCESS
                  and self.API_JSON_RESULTS not in json_response):
                return {}
            elif (self.API_JSON_RESULTS in json_response
                  and self.API_JSON_ERROR_CODE in json_response):
                raise ControllerClientResponseException(
                    json_response[self.API_JSON_RESULTS],
                    json_response[self.API_JSON_ERROR_CODE])
            elif self.API_JSON_ERROR_CODE in json_response:
                raise ControllerClientResponseException(
                    "Request Failed", json_response[self.API_JSON_ERROR_CODE])
            raise ControllerClientResponseException(
                "Request Failed", -1)
        except (HTTPRequestFailedException, HTTPInvalidRequestException) as e:
            self.logger.error(str(e))
            raise ControllerClientResponseException(
                "Server is not accessible", error_codes.server_error)
        except JSONDecodeError as e:
            self.logger.error(str(e))
            raise ControllerClientResponseException(
                "Invalid response from server", error_codes.server_error)

    def create_repo(self, repo_json):
        """
        creates a repo on pachyderm cluster

        :param repo_json:
            information of repo i.e. name and description
        :return:
            returns operation status
        """
        url = f"{self.server_path}/repo"
        response = self.send_http_request(url=url,
                                          http_method=HTTPMethod.POST,
                                          header={"token": self.get_token()},
                                          data=repo_json)
        return response

    # def get_repo(self):
    #     """
    #
    #     :return:
    #     """
    #     url = f"{self.server_path}/repo"
    #     response = self.send_http_request(url=url,
    #                                       http_method=HTTPMethod.GET,
    #                                       header={"token": self.get_token()},
    #                                       data={})
    #     return response

    def create_branch(self, branch_json):
        """
        creates a branch in a repo

        :param branch_json:
            information of branch i.e. repo and branch names
        :return:
            operation status
        """
        url = f"{self.server_path}/repo"
        response = self.send_http_request(url=url,
                                          http_method=HTTPMethod.PUT,
                                          header={"token": self.get_token()},
                                          data=branch_json)
        return response

    def push_dataset(self, dataset_json):
        """
        pushes a dataset into pachyderm cluster

        :param dataset_json:
            information of dataset
        :return:
            operation status
        """
        url = f"{self.server_path}/dataset/manage"
        self.send_http_request(url=url,
                               http_method=HTTPMethod.PUT,
                               header={"token": self.get_token()},
                               data=dataset_json)
        manager = repo_manager.PachydermRepoManager()
        try:
            commit_id = manager.push_files(dataset_json)
            return {"message": f"Dataset push successful. commit id: {commit_id}"}
        except XprExceptions as err:
            return err.message

    def pull_dataset(self, dataset_json):
        """
        pulls a dataset from pachyderm cluster

        :param dataset_json:
            info of the dataset on pachyderm cluster
        :return:
            path of the dataset on user system
        """
        url = f"{self.server_path}/dataset/manage"
        self.send_http_request(url=url,
                               http_method=HTTPMethod.GET,
                               header={"token": self.get_token()},
                               data=dataset_json)
        manager = repo_manager.PachydermRepoManager()
        try:
            dataset_path = manager.manage_xprctl_dataset('pull', dataset_json)
            return {"message": f"Pull Successful, find the files at {dataset_path}"}
        except XprExceptions as err:
            return err.message

    def list_dataset(self, filter_json):
        """
        lists datasets saved on pachyderm cluster as per filter specs

        :param filter_json:
            info to filter required dataset
        :return:
            list of all the files and their props as per filter specs
        """
        url = f"{self.server_path}/dataset/list"
        self.send_http_request(url=url,
                               http_method=HTTPMethod.GET,
                               header={"token": self.get_token()},
                               data=filter_json)
        manager = repo_manager.PachydermRepoManager()
        try:
            dataset_list = manager.manage_xprctl_dataset('list', filter_json)
            return dataset_list
        except XprExceptions as err:
            return err.message
