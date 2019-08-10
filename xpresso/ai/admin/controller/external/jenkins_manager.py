"""
This class implements all the required functionality to use Jenkins
programmatically
"""

__all__ = ['JenkinsManager']
__author__ = 'Naveen Sinha'

import time
import jenkins
import logging
import requests

from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import tostring as ET_tostring

from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    JenkinsConnectionFailedException, JenkinsInvalidInputException
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.logging.xpr_log import XprLogger


class JenkinsManager:
    """
    Jenkins wrapper class to connect to Jenkins server. It can execute any Job
    in the Jenkins, get the current status or help in creating the node.

    Args:
        cfg (XprConfigParser): config for Jenkins.
    """

    JENKINS_SECTION = "jenkins"
    JENKINS_MASTER = "master_host"
    JENKINS_USERNAME = "username"
    JENKINS_PASSWORD = "password"
    JENKINS_TEMPLATE_PIPELINE = "template_job"

    def __init__(self, cfg: XprConfigParser):
        self.config = cfg[self.JENKINS_SECTION]
        self.logger = XprLogger()

        console_handler = logging.StreamHandler()
        self.logger.addHandler(console_handler)
        self.logger.setLevel(logging.DEBUG)
        self.jenkins_server = self.init_connection()

    def init_connection(self) -> jenkins.Jenkins:
        """
        Initiates a connection to Jenkins and returns its objects
        """
        self.logger.info("Initiating connection to Jenkins {}"
                         .format(self.config[self.JENKINS_MASTER]))
        try:
            server = jenkins.Jenkins(
                self.config[self.JENKINS_MASTER],
                username=self.config[self.JENKINS_USERNAME],
                password=self.config[self.JENKINS_PASSWORD]
            )
            self.logger.info("Jenkins connected successfully")
        except jenkins.JenkinsException:
            self.logger.error("Jenkins Connection Failed")
            raise JenkinsConnectionFailedException("Jenkins Connection Failed")
        return server

    def create_pipeline(self, pipeline_name: str, bitbucket_repo: str):
        """
        It creates a pipeline project in the Jenkins master. It uses a template
        to create a pipeline.

        Args:
            bitbucket_repo(str): bitbucket repository from where build will
                                 work
            pipeline_name(str): Jenkins pipeline name identifier

        Returns:
            True. on success Otherwise raise an Exceptions

        Raises
            JenkinsConnectionFailedException, JenkinsInvalidInputException
        """
        self.logger.info("Creating a job named: {}".format(pipeline_name))
        try:
            self.jenkins_server.copy_job(
                self.config[self.JENKINS_TEMPLATE_PIPELINE],
                pipeline_name
            )
            pipeline_config = self.jenkins_server.get_job_config(pipeline_name)
            pipeline_root = ET.fromstring(pipeline_config)
            # Update Description
            self.update_config(pipeline_root=pipeline_root,
                               field_path='description',
                               update_value='Pipeline to perform build for {}'
                               .format(pipeline_name))
            # Update Bit bucket repository
            self.update_config(pipeline_root=pipeline_root,
                               field_path='definition/scm/userRemoteConfigs/'
                                          'hudson.plugins.git.UserRemoteConfig/url',
                               update_value=bitbucket_repo)
            updated_pipeline_config = ET_tostring(pipeline_root).decode()
            self.jenkins_server.reconfig_job(pipeline_name,
                                             updated_pipeline_config)

            # We need to do this because build does not get available for build
            # until job is disabled and enabled
            self.jenkins_server.disable_job(pipeline_name)
            self.jenkins_server.enable_job(pipeline_name)
        except jenkins.JenkinsException:
            self.logger.error("Job Creation Failed")
            raise JenkinsInvalidInputException("Job creation failed")
        except (requests.exceptions.ConnectionError,
                requests.exceptions.SSLError, requests.exceptions.HTTPError):
            self.logger.error("Jenkins Connection Issue")
            raise JenkinsConnectionFailedException("Jenkins Connection Issue")
        self.logger.info("Job Created: {}".format(pipeline_name))

    @staticmethod
    def update_config(pipeline_root: Element,
                      field_path: str, update_value: str):
        """
        Update Jenkins configuration
        Args:
            pipeline_root(Element): Root Tree of the pipeline
            field_path(str): Path to the field which needs to be updated
            update_value(str): value which needs to be updated in the field
        """
        field = pipeline_root.find(field_path)
        field.text = update_value

    def submit_build(self, pipeline_name: str, branch_name: str,
            docker_image_name: str, component_name: str) -> str:
        """
        It executes a job in the Jenkins on the basis of the pipeline name.
        It will return a job id which can be used to fetch the status

        Args:
            docker_image_name(str): docker image name. Image name is tagged with
                                    the build number
            branch_name(str): branch to be build on
            pipeline_name(str): Jenkins pipeline name identifier

        Returns:
            str: Job ID of the pipeline

        Raises
            JenkinsConnectionFailedException, JenkinsInvalidInputException
        """
        self.logger.info("Executing Jenkins build for {} on branch {}"
                         .format(pipeline_name, branch_name))
        try:
            queue_number = self.jenkins_server.build_job(
                pipeline_name,
                parameters={'git_branch': branch_name,
                            'docker_image_name': docker_image_name,
                            'component_name': component_name})

            # Check if build has submitted
            total_attempts = 10
            build_id = self.jenkins_server.get_job_info(
                pipeline_name)['nextBuildNumber']
            while total_attempts:
                total_attempts -= 1
                queue_info = self.jenkins_server.get_queue_item(queue_number)
                if "executable" not in queue_info:
                    self.logger.info("Waiting for build to get submitted")
                    time.sleep(2)
                    continue
                print(queue_info)
                build_id = queue_info["executable"]["number"]
        except jenkins.JenkinsException:
            self.logger.error("Build ID is invalid")
            raise JenkinsInvalidInputException("Build ID is not valid")
        except (requests.exceptions.ConnectionError,
                requests.exceptions.SSLError, requests.exceptions.HTTPError):
            self.logger.error("Jenkins Connection Issue")
            raise JenkinsConnectionFailedException("Jenkins Host is invalid")
        self.logger.info("Build submitted successfully with ID {}"
                         .format(build_id))
        return build_id

    def get_build(self, pipeline_name: str, job_id: int):
        """
        Get the status and details of the job.

        Args:
            pipeline_name(str): Jenkins pipeline name identifier
            job_id(int): Jenkins Job identifier

        Returns:
            dict: details of the jobs.

        Raises
            JenkinsConnectionFailedException, JenkinsInvalidInputException
        """
        self.logger.info("Getting a build detail for {}:{}".format(
            pipeline_name, job_id
        ))
        try:
            build_info = self.jenkins_server.get_build_info(pipeline_name,
                                                            job_id)
        except jenkins.JenkinsException:
            self.logger.exception("Pipeline name and build {}:{} is invalid"
                                  .format(pipeline_name, job_id))
            raise JenkinsInvalidInputException(
                "Pipeline name and build is invalid")
        except (requests.exceptions.ConnectionError,
                requests.exceptions.SSLError, requests.exceptions.HTTPError):
            self.logger.error("Jenkins Connection Issue")
            raise JenkinsConnectionFailedException("Jenkins Connection Issue")
        self.logger.info("Build info received and returned")
        return build_info

    def start_worker_node(self, worker_type: str):
        """
        TODO Starts a worker docker container on any available node to perform
        build
        and deploy

        Args:
            worker_type(str): Name of the worker type. It could be python, java
                              or gpu

        Returns:
            bool: True if worker node is started
        """
        pass

    def check_active_worker_node(self, worker_type: str):
        """
        TODO Check if worker node is active.

        Args:
            worker_type(str): Name of the worker type. It could be python, java
                              or gpu

        Returns:
            bool: True if worker node is active, False Otherwise
        """
        pass

    def delete_pipeline(self, pipeline_name: str):
        """
        Permanently delete the pipeline
        Args:
            pipeline_name: Jenkins pipeline identifier

        Raises
            JenkinsConnectionFailedException, JenkinsInvalidInputException
        """
        self.logger.info("Deleting Pipeline {}".format(pipeline_name))
        try:
            self.jenkins_server.delete_job(pipeline_name)
        except jenkins.JenkinsException:
            self.logger.exception("Pipeline name {} is invalid"
                                  .format(pipeline_name))
            raise JenkinsInvalidInputException("Pipeline name is invalid")
        except (requests.exceptions.ConnectionError,
                requests.exceptions.SSLError, requests.exceptions.HTTPError):
            self.logger.error("Jenkins Connection Issue")
            raise JenkinsConnectionFailedException("Jenkins Connection Issue")
        self.logger.info("Pipeline deleted {}".format(pipeline_name))


