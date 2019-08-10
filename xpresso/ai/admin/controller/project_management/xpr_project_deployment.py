__all__ = ['XprProjectDeployment']
__author__ = 'Sahil Malav'

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.project_management.kubernetes.\
    kubernetes_manager import KubernetesManager
from xpresso.ai.admin.controller.project_management.kubernetes.deployment_factory import \
    DeploymentFactory
from xpresso.ai.admin.controller.project_management.kubeflow.declarative_pipeline.declarative_pipeline_builder \
    import DeclarativePipelineBuilder
from xpresso.ai.admin.controller.project_management.kubeflow.kubeflow_utils \
    import KubeflowUtils
from xpresso.ai.admin.controller.external.gateway.gateway_manager \
    import GatewayManager
from xpresso.ai.admin.controller.utils import project_utils
from xpresso.ai.admin.controller.env_management.env_manager import EnvManager


class XprProjectDeployment:
    """
        This class provides methods for Xpresso Project Deployment management.
    """
    logger = XprLogger()

    def __init__(self, persistence_manager):

        self.persistence_manager = persistence_manager
        self.kubernetes_manager = KubernetesManager(persistence_manager)

    def deploy_project(self, input_project, db_project_info):
        """
        deploys a project onto kubernetes
        Args:
            input_project: project input from the user
            db_project_info: the project information that you get after
                             validation for projects from the database

        Returns: IPs of components deployed

        """

        # deployment now takes an env, instead of a cluster
        # validate whether this version has been deployed to all environments lower than the target
        EnvManager().validate_deployment_target_env(input_project, db_project_info)

        deployment_info = []
        if 'components' in input_project.keys():
            self.logger.info('Components found. Proceeding to deploy '
                             'components.')
            components_result = self.deploy_components(
                input_project, db_project_info)
            deployment_info = deployment_info + components_result
            self.logger.info('Components deployed.')
        if 'pipelines' in input_project.keys():
            self.logger.info('Pipelines found. Proceeding to deploy pipelines.')
            pipeline_result = self.deploy_pipelines(
                input_project, db_project_info[0])
            deployment_info = deployment_info + pipeline_result
        return deployment_info

    def deploy_pipelines(self, input_project, db_project_info):
        """
        deploys pipelines onto Kubeflow
        Args:
            input_project: project input from the user
            db_project_info: the project information that you get after
                             validation for projects from the database
        Returns: IP of the Kubeflow central dashboard

        """
        #cluster = input_project['cluster']
        cluster = EnvManager().get_cluster(input_project['name'], input_project['target_environment'])

        # check if the cluster provided by user for deployment is valid
        cluster_info = self.persistence_manager.find(
            'clusters', {"name": cluster})
        if not cluster_info or not cluster_info[0]['activationStatus']:
            self.logger.error('Required cluster for deployment not found. '
                              'Exiting.')
            raise ClusterNotFoundException(f'Cluster "{cluster}"" does '
                                           f'not exist')
        self.logger.debug('deployment cluster validated')
        master_node = cluster_info[0]['master_nodes']['address']
        pipelines_to_deploy = input_project['pipelines'].items()
        all_pipelines = \
            [pipeline['name'] for pipeline in db_project_info['pipelines']]
        pipeline_ip = []
        for pipeline_name, pipeline_info in pipelines_to_deploy:
            if pipeline_name not in all_pipelines:
                self.logger.error(f"Pipeline '{pipeline_name}' not found.")
                raise PipelineNotFoundException(f"Pipeline '{pipeline_name}' "
                                                f"does not exist.")
            for index, pipeline in enumerate(db_project_info['pipelines']):
                if pipeline['name'] == pipeline_name:
                    pipeline_components = pipeline_info['components'].keys()
                    if not set(pipeline_components) == \
                            set(pipeline['components']):
                        raise ComponentsSpecifiedIncorrectlyException(
                            "Pipeline components were incorrect/incomplete.")
                    pipeline_json = pipeline['declarative_json']
                    declarative_pipeline_builder = DeclarativePipelineBuilder(
                        self.persistence_manager)
                    component_images = project_utils.extract_component_image(
                        db_project_info['components'],
                        pipeline_info['components'])
                    deploy_id = pipeline['deploy_version_id']
                    pipeline_zip = \
                        declarative_pipeline_builder.generate_pipeline_file(
                            pipeline_json, component_images, deploy_id)
                    kubeflow_utils = KubeflowUtils(self.persistence_manager)
                    ambassador_port = \
                        kubeflow_utils.upload_pipeline_to_kubeflow(
                            master_node, 'kubeflow', pipeline_zip)
                    # update the deploy version id in database
                    new_deploy_id = deploy_id + 1
                    self.persistence_manager.update(
                        'projects', {"name": db_project_info["name"]},
                        {f"pipelines.{index}.deploy_version_id": new_deploy_id})
                    pipeline_ip = [{"Pipelines": f"{master_node}:"
                                    f"{ambassador_port}/_/pipeline-dashboard"}]
                    break
        return pipeline_ip

    def deploy_components(self, input_project, db_project_info):
        """
        deploys (non-pipeline) components onto kubernetes
        Args:
            input_project: project input from the user
            db_project_info: the project information that you get after
                             validation for projects from the database:

        Returns: service IP of deployed projects

        """
        self.logger.info('entering deploy_project method with'
                         'arguments {}'.format(input_project))
        target_env = input_project["target_environment"]
        cluster = EnvManager().get_cluster(input_project['name'], target_env)

        # check if the cluster provided by user for deployment is valid
        cluster_info = self.persistence_manager.find(
            'clusters', {"name": cluster})
        if not cluster_info or not cluster_info[0]['activationStatus']:
            self.logger.error('Required cluster for deployment not found. '
                              'Exiting.')
            raise ClusterNotFoundException
        self.logger.debug('deployment cluster validated')

        master_node = cluster_info[0]['master_nodes']['address']
        self.kubernetes_manager.set_api_config(master_node)
        self.kubernetes_manager.check_for_namespace(input_project)

        # deploy all components
        self.logger.debug('Entering loop to deploy components')
        service_ips = []
        try:
            given_components = list(input_project['components'].keys())
        except XprExceptions:
            self.logger.error('Components not specified properly')
            raise ComponentsSpecifiedIncorrectlyException

        all_components = []
        components = db_project_info[0]['components']
        for component_item in components:
            all_components.append(component_item['name'])
        if not set(given_components).issubset(set(all_components)):
            self.logger.error("One or more components specified incorrectly. "
                              "Exiting.")
            raise ComponentsSpecifiedIncorrectlyException(
                "One or more components specified incorrectly.")

        project_name = input_project['name']
        for component in components:
            component_name = component['name']
            if component_name not in given_components:
                continue

            deployment_factory = DeploymentFactory()
            component_deployment = deployment_factory.generate_deployment(
                input_project['components'][component_name], project_name,
                component_name, component['type'])
            component_deployment.project_linux_uid = \
                int(db_project_info[0]["linux_uid"])
            # Iterate through all the build versions and do the deployment for
            # matching build. If nothing match ifnore.
            for index, version in enumerate(component['versions']):
                if version['version_id'] == component_deployment.build_version:
                    docker_image = component['versions'][index]['dockerImage']
                    component_deployment.docker_image = docker_image

                    self.logger.debug('Docker image retrieved.')
                    if component_deployment.is_job():
                        self.kubernetes_manager.kube_deploy_job(
                            component_deployment
                        )
                        service_ips.append({
                            f"{component_deployment.component_name}":
                                "Job created successfully."})
                    elif (component_deployment.is_service() or
                          component_deployment.is_database()):
                        component_deployment.master_node = master_node
                        svc_ip = self.kubernetes_manager.kube_deploy_service(
                            component_deployment
                        )
                        service_ips.append({
                            f"{component_deployment.component_name}": svc_ip})
                        # If this service has is_external field True then we
                        # need to create a new external IP for it
                        self.logger.debug("Checking if is_external is required")
                        if component_deployment.is_external_required() and \
                                svc_ip:
                            self.logger.debug(
                                "Exposing the service outside internet")
                            gateway_manager = GatewayManager()
                            external_ip = \
                                gateway_manager.setup_external_service(
                                    component_name=f"{component_name}--"
                                    f"{project_name}",
                                    internal_service_url=svc_ip)
                            service_ips.append({
                                f"{component_deployment.component_name} "
                                f"External": external_ip})
                            self.logger.debug(f"{external_ip} has been exposed")
                    break
        # Project is associated with an environment where deployment happens.
        if target_env not in db_project_info[0]['deployedEnvironments']:
            # It updates the deployedEnvironments in the DB
            self.logger.debug('adding environment to the list of deployed environments')
            new_list = db_project_info[0]['deployedEnvironments']
            new_list.append(target_env)
            self.persistence_manager.update('projects', {'name': project_name},
                                            {'deployedEnvironments': new_list})
            self.logger.debug('added environment to the list of deployed environments')

        # Mark whether the project is currently deployed in the DB
        if not db_project_info[0]['currentlyDeployed']:
            self.logger.debug('currentlyDeployed status is currently false.')
            self.persistence_manager.update('projects', {'name': project_name},
                                            {'currentlyDeployed': True})
            self.logger.debug('changed currentlyDeployed status to True')

        self.logger.info('Deployment process finished for {}. '
                         'exiting deploy_project method..'.format(project_name))
        return service_ips

    def undeploy_project(self, project, project_info):
        """
        deletes the kubernetes deployment of a deployed project
        Args:
            project: project input from the user
            project_info: the project information that you get after
            token validation for projects

        Returns: status (true/false)

        """
        self.logger.info('entering undeploy_project method with'
                         ' arguments {}'.format(project))
        if not project_info[0]['currentlyDeployed']:
            self.logger.error('Project not deployed currently. Exiting.')
            raise CurrentlyNotDeployedException
        target_env = project['target_environment']
        cluster = EnvManager().get_cluster(project['name'], target_env)
        # checking validity of cluster provided by the user
        cluster_info = self.persistence_manager.find(
            'clusters', {"name": cluster})
        if not cluster_info or not cluster_info[0]['activationStatus'] or \
                target_env not in project_info[0]['deployedEnvironments']:
            self.logger.error('Required cluster for undeployment not found.')
            raise ClusterNotFoundException
        self.logger.debug('Environment and cluster provided by the user validated')
        # get the master node of the cluster
        master_node = cluster_info[0]['master_nodes']['address']
        self.kubernetes_manager.set_api_config(master_node)
        project_name = project['name']

        # call the undeploy function
        self.kubernetes_manager.kube_undeploy(project_name)

        # if this was the only environment on which the project was deployed,
        # then change the deployment status to False, else
        # remove it from the list
        if len(project_info[0]['deployedEnvironments']) == 1:
            self.persistence_manager.update(
                'projects', {'name': project_name},
                {'currentlyDeployed': False, 'deployedEnvironments': []})
            self.logger.debug('Deployment status changed to False')
        else:
            new_list = project_info[0]['deployedEnvironments'].remove(target_env)
            self.persistence_manager.update(
                'projects', {'name': project_name},
                {'deployedEnvironments': new_list})
            self.logger.debug('Environment removed from deployed environments list')
        self.logger.info('exiting undeploy_project method.')
        return True

