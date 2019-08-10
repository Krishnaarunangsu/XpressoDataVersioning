from xpresso.ai.admin.controller.project_management.kubernetes.job_deployment import \
    JobDeployment
from xpresso.ai.admin.controller.project_management.kubernetes.service_deployment import \
    ServiceDeployment

__all__ = 'KubernetesDeploy'
__author__ = 'Sahil Malav'

from kubernetes import client
from kubernetes.client.rest import ApiException
import yaml
import json
import os
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.utils import project_utils


class KubernetesDeploy:
    """
    class containing methods to deploy a project in Kubernetes
    """

    def __init__(self, persistence_manager):

        self.persistence_manager = persistence_manager
        self.logger = XprLogger()
        config_path = XprConfigParser.DEFAULT_CONFIG_PATH
        self.config = XprConfigParser(config_path)
        PROJECTS_SECTION = 'projects'
        DEPLOYMENT_FILES_FOLDER = 'deployment_files_folder'
        self.deployment_files_folder = self.config[PROJECTS_SECTION][
            DEPLOYMENT_FILES_FOLDER]

        if not os.path.isdir(self.deployment_files_folder):
            os.makedirs(self.deployment_files_folder, 0o755)

    def deployment_yaml_generator(self, deployment):
        """
        generates yaml for creating deployment in kubernetes
        Args:
            deployment: Service Deployment
        Returns:
            str: path of yaml generated

        """
        self.logger.info('entering deployment_yaml_generator')
        # converting any special characters to '-'
        project_name = project_utils.modify_string_for_deployment(
            deployment.project_name)
        component = project_utils.modify_string_for_deployment(
            deployment.component_name)
        # this will be the name used in the deployment file
        deployment_name = '{}--{}'.format(project_name, component)
        # reading contents from the standard xpresso deployment yaml file
        with open("config/kubernetes-deployfile.yaml", "r") as f:
            content = f.read()
        yaml_content = self.populate_yaml_content(content, deployment,
                                                  deployment_name)

        filename = "{}/deployfile--{}.yaml".format(self.deployment_files_folder,
                                                   deployment_name)
        with open(filename, "w+") as f:
            yaml.safe_dump(yaml_content, f)
        self.logger.info('exiting deployment_yaml_generator')
        return filename

    def persistent_volume_yaml_generator(self, deployment, persstence_type):
        """
        generates yaml for creating persistent volumne
        Args:
            deployment: Any Deployment
        Returns:
            str: path of yaml generated

        """
        self.logger.info('entering persistent_yaml_generator')

        # converting any special characters to '-'
        project_name = project_utils.modify_string_for_deployment(
            deployment.project_name)
        component = project_utils.modify_string_for_deployment(
            deployment.component_name)
        # this will be the name used in the deployment file
        deployment_name = '{}--{}'.format(project_name, component)

        # reading contents from the standard xpresso deployment yaml file
        with open(f"config/kubernetes-persistent-{persstence_type}.yaml", "r") as f:
            content = f.read()

        content = content.replace("K8_XPRESSO_COMPONENT_NAME",
                                  str(deployment_name))
        content = content.replace("K8_XPRESSO_PERSISTENT_STORAGE_SIZE",
                                  str(deployment.volume_size))
        content = content.replace("K8_XPRESSO_PROJECT_NAME", str(project_name))
        yaml_content = yaml.safe_load(content)

        filename = (f"{self.deployment_files_folder}"
                    f"/persistent-{persstence_type}-file--{deployment_name}.yaml")
        with open(filename, "w+") as f:
            yaml.safe_dump(yaml_content, f)
        self.logger.info('exiting persistent_yaml_generator')
        return filename

    def populate_yaml_content(self, content, deployment, deployment_name):
        content = content.replace("K8_XPRESSO_COMPONENT_NAME",
                                  str(deployment_name))
        content = content.replace("K8_XPRESSO_COMPONENT_REPLICAS",
                                  str(deployment.replicas))
        content = content.replace("K8_XPRESSO_COMPONENT_IMAGE_NAME",
                                  str(deployment.docker_image))
        content = content.replace("K8_XPRESSO_COMPONENT_ENVIRONMENT_LIST",
                                  str(deployment.environment))
        content = content.replace("K8_XPRESSO_PROJECT_LINUX_UID",
                                  str(deployment.project_linux_uid))
        if deployment.need_persistence():
            content = content.replace("K8_XPRESSO_COMPONENT_VOLUME_MOUNT_PATH",
                                      str(deployment.volume_mount_path))

        # content = content.format(deployment_name, replicas, deployment_name,
        #                          image, deployment_name, environment)
        yaml_content = yaml.safe_load(content)

        # Remove persistence if not required
        if not deployment.need_persistence():
            try:
                del yaml_content["spec"]["template"]["spec"]["volumes"]
                del yaml_content["spec"]["template"]["spec"]["containers"][0][
                    "volumeMounts"]
            except (IndexError, KeyError):
                self.logger.warning("spec.template.spec.volumes not found")
        return yaml_content

    def service_yaml_generator(self, project_name, component, port):
        """
        generates yaml for creating service in kubernetes
        Args:
            project_name: project to be deployed
            component: component for which this yaml is generated
            port: array containing info of ports to be opened
        Returns: path of yaml generated

        """
        self.logger.info('entering service_yaml_generator')
        # reading contents from the standard xpresso service yaml file
        with open("config/kubernetes-servicefile.yaml", "r") as f:
            content = f.read()
        # converting any special characters to '-'
        project_name = project_utils.modify_string_for_deployment(project_name)
        component = project_utils.modify_string_for_deployment(component)
        ports = []
        for i in port:
            temp = str(i)
            fixed_port = project_utils.modify_string_for_deployment(
                temp).replace("'", '"')
            ports.append(json.loads(fixed_port))
        # this will be the name used in the service file
        service_name = '{}--{}'.format(project_name, component)
        content = content.format(service_name, ports, service_name)
        yaml_content = yaml.safe_load(content)
        filename = "{}/servicefile--{}.yaml".format(
            self.deployment_files_folder, service_name)
        with open(filename, "w+") as f:
            yaml.safe_dump(yaml_content, f)
        self.logger.info('exiting service_yaml_generator')
        return filename

    def namespace_yaml_generator(self, project_name):
        """
        generates yaml file to create a new namespace
        Args:
            project_name: name of the project to be deployed

        Returns: path of the yaml generated

        """
        self.logger.info('entering namespace_yaml_generator')
        with open("config/kubernetes-namespacefile.yaml", "r") as f:
            content = f.read()
        # converting any special characters to '-'
        project_name = project_utils.modify_string_for_deployment(project_name)
        content = content.format(project_name)
        yaml_content = yaml.safe_load(content)
        filename = "{}/namespacefile--{}.yaml".format(
            self.deployment_files_folder, project_name)
        with open(filename, "w+") as f:
            yaml.safe_dump(yaml_content, f)
        self.logger.info('exiting namespace_yaml_generator')
        return filename

    def job_yaml_generator(self, deployment):
        """
        generates yaml file to create a job
         Args:
            deployment: Any Deployment
        Returns:
            str: path of yaml generated
        """
        self.logger.info('entering job_yaml_generator')
        # reading contents from the standard xpresso job yaml file

        # converting any special characters to '-'
        project_name = project_utils.modify_string_for_deployment(
            deployment.project_name)
        component = project_utils.modify_string_for_deployment(
            deployment.component_name
        )
        # this will be the name used in the job file
        job_name = '{}--{}'.format(project_name, component)

        with open("config/kubernetes-jobfile.yaml", "r") as f:
            content = f.read()

        content = content.replace("K8_XPRESSO_COMPONENT_NAME", str(job_name))
        content = content.replace("K8_XPRESSO_COMPONENT_IMAGE_NAME",
                                  str(deployment.docker_image))
        content = content.replace("K8_XPRESSO_COMPONENT_ENVIRONMENT_LIST",
                                  str(deployment.environment))
        content = content.replace("K8_XPRESSO_COMPONENT_COMMAND",
                                  str(deployment.commands))
        content = content.replace("K8_XPRESSO_COMPONENT_REPLICAS",
                                  str(deployment.replicas))

        if deployment.need_persistence():
            content = content.replace("K8_XPRESSO_COMPONENT_VOLUME_MOUNT_PATH",
                                      str(deployment.volume_mount_path))
        # content = content.format(job_name, job_name, image, environment,
        #                          command, parallelism)
        yaml_content = yaml.safe_load(content)
        # Remove persistence if not required
        if not deployment.need_persistence():
            try:
                del yaml_content["spec"]["template"]["spec"]["volumes"]
                del yaml_content["spec"]["template"]["spec"]["containers"][0][
                    "volumeMounts"]
            except (IndexError, KeyError):
                self.logger.warning("spec.template.spec.volumes not found")
        filename = "{}/jobfile--{}.yaml".format(
            self.deployment_files_folder, job_name)
        with open(filename, "w+") as f:
            yaml.safe_dump(yaml_content, f)
        self.logger.info('exiting job_yaml_generator')
        return filename

    def cronjob_yaml_generator(self, project_name, component, schedule,
                               image, environment, args):
        """
        generates yaml file to create a cronjob
        :param environment: environment
        :param project_name: project name
        :param component: component name
        :param schedule: Cron Job schedule in standard Cron format
        :param image: docker image
        :param args: array of args to run
        :return: path of yaml generated
        """
        self.logger.info('entering cronjob_yaml_generator')
        if not project_utils.validate_cronjob_format(schedule):
            self.logger.error('Invalid cron schedule provided. Exiting.')
            raise InvalidCronScheduleException
        # reading contents from the standard xpresso cronjob yaml file
        with open("config/kubernetes-cronjobfile.yaml", "r") as f:
            content = f.read()
        # converting any special characters to '-'
        project_name = project_utils.modify_string_for_deployment(project_name)
        component = project_utils.modify_string_for_deployment(component)
        # this will be the name used in the job file
        cronjob_name = '{}--{}'.format(project_name, component)
        content = content.format(cronjob_name, schedule, cronjob_name,
                                 image, environment, args)
        yaml_content = yaml.safe_load(content)
        filename = "{}/cronjobfile--{}.yaml".format(
            self.deployment_files_folder, cronjob_name)
        with open(filename, "w+") as f:
            yaml.safe_dump(yaml_content, f)
        self.logger.info('exiting cronjob_yaml_generator')
        return filename

    def patch_deployment_client(self, path, project_name):
        """
        helper function to patch deployment for project as a given yaml file on
        Kubernetes via the Kubernetes API
        Args:
            path: path of the yaml to be deployed
            project_name: project to be deployed (needed for namespace)
        :return: status of patching (True/Error Code)
        """
        self.logger.info('entering patch_deploy_client')
        try:
            with open(path) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.ExtensionsV1beta1Api()
                # collecting response from API
                r = k8s_beta.patch_namespaced_deployment(
                    name=dep['metadata']['name'],
                    body=dep,
                    namespace=project_utils.modify_string_for_deployment(
                        project_name))
                self.logger.debug(
                    "Deployment patched. Details : {}".format(str(r)))
            self.logger.info('exiting patch_deploy_client')
            return True
        except ApiException as e:
            self.logger.error('Patching deployment failed. '
                              'Error info : {}.'.format(e))
            raise DeploymentCreationFailedException

    def deploy_client(self, path, project_name):
        """
        helper function to create deployment for a given yaml file on
        Kubernetes via the Kubernetes API
        Args:
            path: path of the yaml to be deployed
            project_name: project to be deployed (needed for namespace)

        Returns: status of deployment (True/Error Code)

        """
        self.logger.info('entering deploy_client')
        try:
            with open(path) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.ExtensionsV1beta1Api()
                # collecting response from API
                r = k8s_beta.create_namespaced_deployment(
                    body=dep,
                    namespace=project_utils.modify_string_for_deployment(
                        project_name))
                self.logger.debug(
                    "Deployment created. Details : {}".format(str(r)))
            self.logger.info('exiting deploy_client')
            return True
        except ApiException as e:
            if e.status == 409:  # in case of conflict, patch the deployment
                self.patch_deployment_client(path, project_name)
                return True
            self.logger.error('Creation of deployment failed. Exiting.')
            raise DeploymentCreationFailedException

    def patch_service_client(self, path, project_name):
        """
                helper function to patch service for project as a given yaml
                file on Kubernetes via the Kubernetes API
                Args:
                    path: path of the yaml to be deployed
                    project_name: project to be deployed (needed for namespace)

                Returns: status of service patching (True/Error code)

                """
        self.logger.info('entering patch_service_client')
        try:
            with open(path) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.CoreV1Api()
                # collecting response from API
                r = k8s_beta.patch_namespaced_service(
                    namespace=project_utils.modify_string_for_deployment(
                        project_name), body=dep, name=dep['metadata']['name'])
                self.logger.debug(
                    "Service patched. Details : {}".format(str(r)))
            self.logger.info('exiting patch_service_client')
            return True
        except ApiException as e:
            self.logger.error('Patching service failed. Error details : '
                              '{}'.format(e))
            if e.status == 422:  # Unprocessable Entity
                self.logger.error("Can't patch service port.")
                raise PortPatchingAttemptedException
            raise ServiceCreationFailedException

    def create_service_client(self, path, project_name):
        """
        helper function to create service for a given yaml file on
        Kubernetes via the Kubernetes API
        Args:
            path: path of the yaml to be deployed
            project_name: project to be deployed (needed for namespace)

        Returns: status of service creation (True/Error code)

        """
        self.logger.info('entering create_service_client')
        try:
            with open(path) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.CoreV1Api()
                # collecting response from API
                r = k8s_beta.create_namespaced_service(
                    namespace=project_utils.modify_string_for_deployment(
                        project_name), body=dep)
                self.logger.debug(
                    "Service created. Details : {}".format(str(r)))
            self.logger.info('exiting create_service_client')
            return True
        except ApiException as e:
            if e.status == 409:
                self.patch_service_client(path, project_name)
                return True
            self.logger.error('Creation of service failed. Exiting.')
            raise ServiceCreationFailedException

    def create_namespace_client(self, path):
        """
        helper function to create namespace for a given yaml file on
        Kubernetes via the Kubernetes API
        Args:
            path: path of the yaml

        Returns: status of namespace creation (True/Error Code)

        """
        self.logger.info('entering create_namespace_client')
        try:
            with open(path) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.CoreV1Api()
                r = k8s_beta.create_namespace(body=dep)
                self.logger.debug(
                    "Namespace created. Details : {}".format(str(r)))
            self.logger.info('exiting create_namespace_client')
            return True
        except:
            self.logger.error('Failed to create namespace. Exiting.')
            raise NamespaceCreationFailedException

    def patch_job_client(self, path, project_name):
        self.logger.info('entering patch_job_client')
        try:
            with open(path) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.BatchV1Api()
                # collecting response from API
                r = k8s_beta.patch_namespaced_job(
                    name=dep['metadata']['name'],
                    body=dep,
                    namespace=project_utils.modify_string_for_deployment(
                        project_name))
                self.logger.debug(
                    "Job patched. Details : {}".format(str(r)))
            self.logger.info('exiting patch_job_client')
            return True
        except ApiException as e:
            self.logger.error('Patching job failed. '
                              'Error info : {}.'.format(e))
            raise JobCreationFailedException

    def create_job_client(self, path, project_name):
        """
        method to create a job in kubernetes
        :param path: path of the yaml file
        :param project_name: project name of which the job is a part
        :return: status (True/Error code)
        """
        self.logger.info('Entering create_job_client')
        try:
            with open(path) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.BatchV1Api()
                # collecting response from API
                r = k8s_beta.create_namespaced_job(
                    namespace=project_utils.modify_string_for_deployment(
                        project_name), body=dep)
                self.logger.debug(
                    "Job created. Details : {}".format(str(r)))
            self.logger.info('exiting create_job_client')
            return True
        except ApiException as e:
            if e.status == 409:  # in case of conflict, patch the job
                self.patch_job_client(path, project_name)
                return True
            self.logger.error('Creation of job failed. Exiting.')
            raise JobCreationFailedException

    def patch_cronjob_client(self, path, project_name):
        self.logger.info('entering patch_cronjob_client')
        try:
            with open(path) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.BatchV1beta1Api()
                # collecting response from API
                r = k8s_beta.patch_namespaced_cron_job(
                    name=dep['metadata']['name'],
                    body=dep,
                    namespace=project_utils.modify_string_for_deployment(
                        project_name))
                self.logger.debug(
                    "CronJob patched. Details : {}".format(str(r)))
            self.logger.info('exiting patch_cronjob_client')
            return True
        except ApiException as e:
            self.logger.error('Patching cronjob failed. '
                              'Error info : {}.'.format(e))
            raise CronjobCreationFailedException

    def create_cronjob_client(self, path, project_name):
        """
                method to create a cronjob in kubernetes
                :param path: path of the yaml file
                :param project_name: project name of which the cronjob is a part
                :return: status (True/Error code)
                """
        self.logger.info('Entering create_cronjob_client')
        try:
            with open(path) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.BatchV1beta1Api()
                # collecting response from API
                r = k8s_beta.create_namespaced_cron_job(
                    namespace=project_utils.modify_string_for_deployment(
                        project_name), body=dep)
                self.logger.debug(
                    "Cron Job created. Details : {}".format(str(r)))
            self.logger.info('exiting create_cronjob_client')
            return True
        except ApiException as e:
            if e.status == 409:  # in case of conflict, patch the cronjob
                self.patch_cronjob_client(path, project_name)
                return True
            self.logger.error('Creation of cron job failed. Exiting.')
            raise CronjobCreationFailedException

    def get_service_ip(self, deployment: ServiceDeployment):
        """
        method to get the list of IP addresses for services of a component
        Args:
            deployment: Service Depoyment Object

        Returns: array of service IPs

        """
        self.logger.info('Entering get_service_ip method')
        service_name = '{}--{}'.format(
            project_utils.modify_string_for_deployment(deployment.project_name),
            project_utils.modify_string_for_deployment(
                deployment.component_name))
        k8s_beta = client.CoreV1Api()
        r = k8s_beta.read_namespaced_service(
            name=service_name,
            namespace=project_utils.modify_string_for_deployment(
                deployment.project_name))

        service_ips = []
        for port in r.spec.ports:
            service_ips.append('{}:{}'.format(deployment.master_node,
                                              port.node_port))
        self.logger.info('Exiting get_service_ip method')
        return service_ips

    def patch_persistence_volume(self, pv):
        """
        Helper function to patch persistence volume
        Args:
            pv: persistence volume yaml file
        :return: status of patching (True/Error Code)
        """
        self.logger.info('entering persistence')
        try:
            with open(pv) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.CoreV1Api()
                # collecting response from API
                r = k8s_beta.patch_persistent_volume(
                    name=dep['metadata']['name'],
                    body=dep)
                self.logger.debug(
                    "Persistence volume patched. Details : {}".format(str(r)))
            self.logger.info('exiting patch_deploy_client')
            return True
        except ApiException as e:
            self.logger.error('Patching PV failed. '
                              'Error info : {}.'.format(e))
            raise DeploymentCreationFailedException

    def patch_persistence_volume_claim(self, pv, pvc, project_name):
        """
        Helper function to patch persistence volume claim
        Args:
            pv: persistence volume yaml file
            pvc: persistence volumet claim yaml fil
            project_name: project to be deployed (needed for namespace)
        :return: status of patching (True/Error Code)
        """
        self.logger.info('entering persistence')
        try:
            with open(pvc) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.CoreV1Api()
                # collecting response from API
                r = k8s_beta.patch_namespaced_persistent_volume_claim(
                    name=dep['metadata']['name'],
                    body=dep,
                    namespace=project_utils.modify_string_for_deployment(
                        project_name))
                self.logger.debug(
                    "Persistence volume patched. Details : {}".format(str(r)))
            self.logger.info('exiting patch_deploy_client')
            return True
        except ApiException as e:
            self.logger.error('Patching PVC failed. '
                              'Error info : {}.'.format(e))
            raise DeploymentCreationFailedException

    def create_persistence_if_required(self, deployment):
        """ Check if persistence is required, If yes then create one"""
        self.logger.debug("Checking for persistence")
        if not deployment.need_persistence():
            self.logger.debug("Persistence not needed.")
            return False

        self.logger.info("Persistence is needed")
        pv = self.persistent_volume_yaml_generator(deployment,
                                                   persstence_type="volume")
        pvc = self.persistent_volume_yaml_generator(deployment,
                                                   persstence_type="volume-claim")
        try:
            with open(pv) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.CoreV1Api()
                # collecting response from API
                r = k8s_beta.create_persistent_volume(
                    body=dep)
                self.logger.debug(
                    f"Persistence Volume created. Details : {str(r)}")
        except ApiException as e:
            if e.status == 409:  # in case of conflict, patch the deployment
                self.patch_persistence_volume(pv)
                return True
            self.logger.error('Creation of PV failed. Exiting.')
            raise DeploymentCreationFailedException

        try:
            with open(pvc) as f:
                dep = yaml.safe_load(f)
                k8s_beta = client.CoreV1Api()
                # collecting response from API
                r = k8s_beta.create_namespaced_persistent_volume_claim(
                    body=dep,
                    namespace=project_utils.modify_string_for_deployment(
                              deployment.project_name))
                self.logger.debug(
                    f"Persistence Volume Claim created. Details : {str(r)}")

            self.logger.info('exiting deploy_client')
            return True
        except ApiException as e:
            if e.status == 409:  # in case of conflict, patch the deployment
                self.patch_persistence_volume_claim(pv, pvc,
                                                    deployment.project_name)
                return True
            self.logger.error('Creation of PVC failed. Exiting.')
            raise DeploymentCreationFailedException

    def run_deployment_steps(self, deployment: ServiceDeployment):
        try:
            self.create_persistence_if_required(deployment)
            deployment_yaml = self.deployment_yaml_generator(
                deployment)
            self.deploy_client(deployment_yaml, deployment.project_name)
            self.logger.debug(f'Deployment created for '
                              f'{deployment.component_name}. '
                              f'Now creating service.')
            service_yaml = self.service_yaml_generator(
                deployment.project_name,
                deployment.component_name,
                deployment.ports)
            self.create_service_client(service_yaml, deployment.project_name)
            self.logger.debug(f'Service created for '
                              f'{deployment.component_name}')
            return True
        except XprExceptions:
            self.logger.error('Error while running deployment steps. '
                              'Deployment failed.')
            raise ProjectDeploymentFailedException

    def run_job_steps(self, deployment: JobDeployment):
        if deployment.is_base_job():
            try:
                self.create_persistence_if_required(deployment)
                job_yaml = self.job_yaml_generator(deployment)
                self.create_job_client(job_yaml, deployment.project_name)
                self.logger.debug(f'Job created for '
                                  f'{deployment.component_name}')
            except XprExceptions:
                self.logger.error('Error while running job steps. '
                                  'Job creation failed.')
                raise JobCreationFailedException
        elif deployment.is_cronjob():
            try:
                self.create_persistence_if_required(deployment)
                cronjob_yaml = self.cronjob_yaml_generator(deployment)
                self.create_cronjob_client(cronjob_yaml,
                                           deployment.project_name)
                self.logger.debug(f'Cronjob created for '
                                  f'{deployment.component_name}')
            except XprExceptions:
                self.logger.error('Error while running job steps. '
                                  'Cronjob creation failed.')
                raise CronjobCreationFailedException
