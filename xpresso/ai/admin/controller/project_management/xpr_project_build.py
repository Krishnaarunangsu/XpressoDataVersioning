__all__ = ['XprProjectBuild']
__author__ = 'Sahil Malav'


from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.external.jenkins_manager import JenkinsManager
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *


class XprProjectBuild:
    """
        This class provides methods for Xpresso Project Build management.
    """
    logger = XprLogger()

    def __init__(self, persistence_manager):
        config_path = XprConfigParser.DEFAULT_CONFIG_PATH
        config = XprConfigParser(config_path)

        self.jenkins = JenkinsManager(config)
        self.persistence_manager = persistence_manager

    def get_build_version(self, project_info):
        """

        Args:
            project_info: the project information that you get after
            token validation for projects

        Returns: an array containing build versions history of all components

        """
        self.logger.info('entering get_build_version method')
        components = project_info[0]['components']
        versions = []
        for i in components:
            versions.append({'component_name': i['name'],
                             'versions': i['versions']})
        self.logger.info('Success. Exiting get_build_version method.')
        return versions

    def build_project(self, project, project_info):
        """
        Builds the requested components of a project
        Args:
            project: project input from the user
            project_info: the project information that you get after
            token validation for projects

        Returns: an array containing build IDs of the components

        """
        self.logger.info('entering build_project method')
        try:
            components = list(project['components'].keys())
        except XprExceptions:
            self.logger.error('Components not specified properly')
            raise ComponentsSpecifiedIncorrectlyException
        all_components = []
        for i in project_info[0]['components']:
            all_components.append(i['name'])
        if not set(components).issubset(set(all_components)):
            self.logger.error("One or more components specified incorrectly. "
                              "Exiting.")
            raise ComponentsSpecifiedIncorrectlyException
        build_ids = []
        project_name = project['name']
        self.logger.debug('Entering loop to build all the requested components')
        try:
            for i in components:  # iterating over components to build
                for index, component in enumerate(
                        project_info[0]['components']):
                    if component['name'] == i:
                        try:
                            branch = project['components'][i]['branch']
                        except XprExceptions:
                            self.logger.error(
                                'Invalid branch info for {}'.format(i))
                            raise BranchNotSpecifiedException
                        try:
                            description = \
                                project['components'][i]['description']
                        except XprExceptions:
                            self.logger.error('Invalid description')
                            raise IncompleteProjectInfoException

                        docker_prefix = component['dockerPrefix']
                        docker_name = '{}{}'.format(docker_prefix, branch)
                        jenkins_id = self.jenkins.submit_build(
                            f'{project_name}__{i}', branch, docker_name, i)
                        build_ids.append({i: jenkins_id})
                        # generate docker image name
                        docker_image = '{}:{}'.format(docker_name, jenkins_id)
                        current_version = {"version_id": jenkins_id,
                                           "version_description": description,
                                           "dockerImage": docker_image}
                        versions = component['versions']
                        versions.append(current_version)  # update version array
                        self.persistence_manager.update(
                            'projects', {"name": project_name},
                            {"components.{}.versions".format(index): versions})
                        break
            return build_ids
        except XprExceptions:
            self.logger.error('Build failed!')
            raise BuildRequestFailedException
