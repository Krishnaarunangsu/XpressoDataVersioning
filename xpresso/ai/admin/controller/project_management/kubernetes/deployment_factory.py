""" Factory class for generating Deployment """

from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.project_management.kubernetes.deployment import \
    Deployment
from xpresso.ai.admin.controller.project_management.kubernetes.job_deployment import \
    JobDeployment
from xpresso.ai.admin.controller.project_management.kubernetes.service_deployment import \
    ServiceDeployment
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser

__all__ = ["DeploymentFactory"]
__author = ["Naveen Sinha"]


class DeploymentFactory:
    """
    Factory class which generates the specific deployment object
    depending on the project json provided.
    It takes a component json and returns an object of relevant deployment
    object
    """

    JOB_COMPONENTS = ["job", "ds_job"]

    def __init__(self):
        self.config = XprConfigParser()
        self.logger = XprLogger()

    def generate_deployment(self, input_component_json, project_name,
                            component_name, component_type) -> Deployment:
        """
        Parses the input project json and create a deployment object

        Args:
            component_name: Name of the component
            component_type: Component type described during project creation
            project_name: Name of the project
            input_component_json: input json for the component
        Returns:
            Deployment: Object Deployment
        """
        self.logger.debug('Deploying {}'.format(component_name))
        if component_type in self.JOB_COMPONENTS:
            deployment = JobDeployment(project_name, component_name)
        else:
            deployment = ServiceDeployment(project_name, component_name)

        deployment.replicas = self.get_value(input_component_json, 'replicas',
                                             ComponentsSpecifiedIncorrectlyException)

        # verify build version
        deployment.build_version = self.get_value(input_component_json, 'build_version',
                                                  exception=InvalidBuildVersionException)

        deployment.environment = self.get_value(input_component_json, 'environment',
                                                exception=None, default=[])

        deployment.persistence = []
        try:
            deployment.persistence = input_component_json['persistence']
            if deployment.persistence:
                deployment.volume_size = deployment.persistence[0]["size"]
                deployment.volume_mount_path = deployment.persistence[0]["mount_path"]
        except (IndexError, KeyError, XprExceptions):
            self.logger.error('Persistence specified incorrectly. Ignoring')

        if deployment.is_job():

            deployment.job_type = self.get_value(input_component_json, 'type',
                                                 exception=InvalidJobTypeException)

            deployment.schedule = self.get_value(input_component_json, 'cron_schedule',
                                                 exception=InvalidCronScheduleException)

            deployment.commands = self.get_value(input_component_json, 'commands',
                                                 exception=InvalidJobCommandsException)
        elif deployment.is_service() or deployment.is_database():
            # verify service ports
            deployment.ports = self.get_value(input_component_json, 'ports',
                                              exception=ComponentsSpecifiedIncorrectlyException)
            if not deployment.ports:
                self.logger.error('Ports are empty')
                raise ComponentsSpecifiedIncorrectlyException
            # Verify if external service required
            deployment.is_external = self.get_value(input_component_json, 'is_external',
                                                    exception=None, default=False)
        return deployment

    def get_value(self, input_component_json, key, exception, default=None):
        """ Fetch value from the input component json for a given key.
        If not found raise the given exception.
        If exception is None, return the default value"""
        try:
            return input_component_json[key]
        except (IndexError, KeyError, XprExceptions):
            error_msg = f'Components specified incorrectly. {key} invalid'
            if not exception:
                self.logger.warn(error_msg)
                return default
            self.logger.error(error_msg)
            raise exception(message=error_msg)
