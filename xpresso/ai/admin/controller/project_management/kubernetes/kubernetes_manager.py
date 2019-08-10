__all__ = 'KubernetesDeploy'
__author__ = 'Sahil Malav'

from kubernetes import client
from kubernetes.client.rest import ApiException
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.project_management.kubernetes.kube_deploy_helper \
    import KubernetesDeploy
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.utils import project_utils
from xpresso.ai.admin.controller.project_management.kubernetes.deployment \
    import Deployment
from xpresso.ai.admin.controller.project_management.kubernetes.job_deployment \
    import JobDeployment
from xpresso.ai.admin.controller.project_management.kubernetes.service_deployment\
    import ServiceDeployment


class KubernetesManager:

    def __init__(self, persistence_manager):

        self.persistence_manager = persistence_manager
        self.kubernetes_deploy = KubernetesDeploy(persistence_manager)
        self.logger = XprLogger()

    def set_api_config(self, master_node):
        """
        Sets the kubernetes API config
        :param master_node: IP of master node of the cluster on which
        project is to be deployed
        :return: nothing
        """
        # get the master node of the cluster
        master_info = self.persistence_manager.find(
            'nodes', {"address": master_node})
        # get the kubernetes bearer token from master
        try:
            token = master_info[0]['token']
        except (IndexError, KeyError):
            self.logger.error("Cluster is not valid. No valid node exist")
            raise ClusterNotFoundException("Cluster is invalid")
        self.logger.debug('Bearer token retrieved from master node')
        # kubernetes API configurations
        configuration = client.Configuration()
        configuration.host = 'https://{}:6443'.format(master_node)
        configuration.verify_ssl = False
        configuration.debug = True
        configuration.api_key = {"authorization": "Bearer " + token}
        client.Configuration.set_default(configuration)
        self.logger.debug('API configurations set.')

    def check_for_namespace(self, project):
        """
        Check if a namespace exists for the given project.  If not, creates it.
        :param project: project to be deployed
        :return: nothing
        """
        # check if namespace exists for the project
        self.logger.debug('checking for existing namespace')
        namespaces = client.CoreV1Api().list_namespace()
        flag = False
        project_name = project['name']
        for ns in namespaces.items:
            if ns.metadata.name == \
                    project_utils.modify_string_for_deployment(project_name):
                flag = True
                self.logger.debug('Namespace for project already exists.')
        if not flag:  # if project_name not in namespaces
            self.logger.debug('creating namespace for the project')
            ns_path = self.kubernetes_deploy.namespace_yaml_generator(
                project_name)
            # create namespace for project
            self.kubernetes_deploy.create_namespace_client(ns_path)

    def kube_deploy_job(self, deployment: Deployment):
        """
        Deploys a job/cronjob component.
        Args:
            deployment: Job deployment object
        """
        if not isinstance(deployment, JobDeployment):
            raise ComponentsSpecifiedIncorrectlyException("Service component"
                                                          "is invalid")
        self.logger.info('entering kube_deploy_job')
        self.logger.debug('running job steps')
        self.kubernetes_deploy.run_job_steps(deployment)

    def kube_deploy_service(self, deployment: Deployment):
        """
        Deploys a service component.
        Args:
            deployment: Service deployment object
        Returns:
            str: IP of the hosted service
        """
        if not isinstance(deployment, ServiceDeployment):
            raise ComponentsSpecifiedIncorrectlyException("Service component"
                                                          "is invalid")
        self.logger.debug('running deployment steps')
        self.kubernetes_deploy.run_deployment_steps(deployment)
        service_ip = self.kubernetes_deploy.get_service_ip(deployment)
        return service_ip

    def kube_undeploy(self, project_name):
        """
        Undeploys a project
        :param project_name: project to be undeployed
        :return: nothing
        """
        try:
            self.logger.debug('Deleting namespace to undeploy project')
            k8s_beta = client.CoreV1Api()
            resp = k8s_beta.delete_namespace(
                project_utils.modify_string_for_deployment(project_name))
            self.logger.debug(
                "Namespace deleted. Details : {}".format(str(resp)))
        except ApiException as e:
            if e.status == 404:     # Not found
                self.logger.error('Project is not deployed currently.')
                raise CurrentlyNotDeployedException
