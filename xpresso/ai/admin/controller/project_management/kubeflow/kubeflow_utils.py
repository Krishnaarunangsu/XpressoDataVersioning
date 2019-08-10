__all__ = 'KubeflowUtils'
__author__ = 'Sahil Malav'


import kfp_server_api
from kfp_server_api.rest import ApiException as KFApiException
from kubernetes import client
from kubernetes.client.rest import ApiException
from xpresso.ai.admin.controller.project_management.kubernetes.\
    kubernetes_manager import KubernetesManager
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *


class KubeflowUtils:

    def __init__(self, persistence_manager):
        self.persistence_manager = persistence_manager
        self.kubernetes_manager = KubernetesManager(persistence_manager)
        self.logger = XprLogger()

    def install_kubeflow(self, master_node, namespace):
        """
        installs kubeflow on a given node, in a given namespace
        Args:
            master_node: master node of the cluster
            namespace: namespace in which kubeflow is to be installed

        Returns: nothing

        """
        pass

    def fetch_ambassador_port(self, master_node, namespace):
        """
        Fetches the port on which ambassador is running
        Args:
            master_node: master node IP of the cluster
            namespace: namespace on which ambassador is deployed

        Returns: ambassador nodePort

        """
        self.logger.info('entering change_ambassador_port method')
        self.kubernetes_manager.set_api_config(master_node)
        k8s_beta = client.CoreV1Api()
        try:
            self.kubernetes_manager.set_api_config(master_node)
            s = k8s_beta.read_namespaced_service(name='ambassador',
                                                 namespace=namespace)
            ambassador_port = s.spec.ports[0].node_port
        except ApiException as e:
            self.logger.error(f'Ambassaddor port fetching failed. Details : '
                              f'{e.status, e.body}')
            raise AmbassadorPortFetchException('Failed to fetch pipeline port.')
        self.logger.info('exiting fetch_ambassador_port method')
        return ambassador_port

    def set_kubeflow_api_config(self, master_node, ambassador_port):
        """
        sets the Kubeflow API config
        Args:
            ambassador_port: ambassador's service nodePort
            master_node: address of the master node

        Returns: nothing

        """
        self.logger.info('entering set_kubeflow_api_config method')
        try:
            master_info = self.persistence_manager.find(
                'nodes', {"address": master_node})
            token = master_info[0]['token']
        except (IndexError, KeyError):
            self.logger.error("Token retrieval from master node failed.")
            raise IncorrectTokenException(
                "Token retrieval from master node failed.")
        config = kfp_server_api.configuration.Configuration()
        config.verify_ssl = False
        config.debug = True
        config.host = f'http://{master_node}:{ambassador_port}/pipeline'
        config.api_key = {"authorization": "Bearer " + token}
        self.logger.info('exiting set_kubeflow_api_config method')
        return config

    def upload_pipeline_to_kubeflow(self, master_node, namespace, pipeline_zip):
        """
        uploads given kubeflow pipeline on the given cluster
        Args:
            namespace: namespace on which kubeflow is installed
            master_node: master node IP of the cluster
            pipeline_zip: zip file containing the pipeline yaml

        Returns: ambassador nodePort

        """
        self.logger.info('entering upload_pipeline_to_kubeflow method')
        ambassador_port = self.fetch_ambassador_port(master_node, namespace)
        self.logger.debug('fetched ambassador port')
        config = self.set_kubeflow_api_config(master_node, ambassador_port)
        api_client = kfp_server_api.api_client.ApiClient(config)
        try:
            upload_client = kfp_server_api.api.PipelineUploadServiceApi(
                api_client)
            upload_client.upload_pipeline(pipeline_zip)
        except KFApiException as e:
            if e.status == 500:
                self.logger.error('Trying to upload already existing pipeline')
                raise PipelineUploadFailedException(
                    'Pipeline already exists. Please choose a different name.')
            else:
                self.logger.error(f'Pipeline upload failed. Reason : {e.body}')
                raise PipelineUploadFailedException(e.body)
        return ambassador_port
