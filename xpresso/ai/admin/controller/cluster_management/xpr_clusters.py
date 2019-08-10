__all__ = ['XprClusters']
__author__ = 'Sahil Malav'

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.cluster_management.cluster import Cluster
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.env_management.env_manager import EnvManager


class XprClusters:
    """
    This class provides methods for Xpresso cluster management.
    """

    def __init__(self, persistence_manager):
        self.logger = XprLogger()
        self.persistence_manager = persistence_manager

    def get_clusters(self, cluster):
        """
        Retrieves info about specified cluster.
        Args:
            cluster: cluster name

        Returns: dictionary object with cluster info

        """
        self.logger.info('entering get_clusters method with'
                         ' input {}'.format(cluster))
        if cluster == {}:
            self.logger.debug('getting all clusters')
            clusters = self.persistence_manager.find('clusters', {})
            all_clusters = []
            for current_cluster in clusters:
                try:
                    temp = dict(name=current_cluster['name'],
                                activationStatus=current_cluster['activationStatus'],
                                master_nodes=current_cluster['master_nodes'],
                                worker_nodes=current_cluster['worker_nodes'])

                    all_clusters.append(temp)
                except KeyError:
                    self.logger.error(f"Invalid cluster format {current_cluster}")
            self.logger.info(
                'exiting get_clusters method with list of all clusters')
            return all_clusters
        else:
            self.logger.debug('getting specific cluster(s)')
            info = self.persistence_manager.find('clusters', cluster)
            if not info:
                self.logger.info('exiting get_clusters method with empty list')
                return []
            for item in info:
                if "_id" in item:
                    del item["_id"]
            self.logger.info(
                'exiting get_clusters method with required cluster(s)')
            return info

    def deactivate_cluster(self, cluster):
        """
        removes specified cluster from the database
        Args:
            cluster: cluster name

        Returns: count of items deleted

        """
        self.logger.info('entering deactivate_cluster method '
                         'with input {}'.format(cluster))
        if 'name' not in cluster:
            self.logger.error('Cluster name not provided.')
            raise IncompleteClusterInfoException
        cluster_name = cluster['name']
        self.logger.debug('Checking for already existing cluster.')
        check = self.persistence_manager.find('clusters',
                                              {"name": cluster_name})
        if not check or not check[0]['activationStatus']:
            self.logger.error('Cluster does not exist')
            raise ClusterNotFoundException
        self.persistence_manager.update('clusters', {"name": cluster['name']},
                                        {"activationStatus": False})
        self.logger.info('exiting deactivate_cluster method.')

        # let environment manager know that the cluster has been deactivated
        EnvManager().deactivate_cluster(cluster)
        return True

    def register_cluster(self, cluster):
        """
        registers a new cluster in database
        Args:
            cluster: cluster to be registered

        Returns: cluster's id

        """
        self.logger.info('entering register_cluster method '
                         'with input {}'.format(cluster))
        new_cluster = Cluster(cluster)
        new_cluster.validate_mandatory_fields()
        if not cluster['name']:
            self.logger.error('Cluster name cannot be blank. Exiting.')
            raise ClusterNameBlankException
        check = self.persistence_manager.find('clusters',
                                              {"name": cluster['name']})
        if check and not check[0]["activationStatus"]:
            self.persistence_manager.update(
                'clusters', {"name": cluster['name']},
                {"activationStatus": True})
            return str(check[0]['_id'])
        if 'master_nodes' in cluster:
            new_cluster.set('master_nodes', cluster['master_nodes'])
        else:
            new_cluster.set('master_nodes', [])
        if 'worker_nodes' in cluster:
            new_cluster.set('worker_nodes', cluster['worker_nodes'])
        else:
            new_cluster.set('worker_nodes', [])
        new_cluster.set('activationStatus', True)
        try:
            inserted_id = self.persistence_manager.insert('clusters', cluster,
                                                          False)
            self.logger.info('exiting register_cluster method '
                             'with insert ID {}'.format(str(inserted_id)))
            return str(inserted_id)
        except UnsuccessfulOperationException:
            self.logger.error('Cluster already exists. Exiting.')
            raise ClusterAlreadyExistsException('cluster already exists')
