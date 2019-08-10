""" User Metrics class"""
from xpresso.ai.admin.controller.metrics.abstract_metrics import AbstractMetrics

__all__ = ["ClusterMetrics"]
__author__ = ["Naveen Sinha"]


class ClusterMetrics(AbstractMetrics):
    """
    Fetches all the details for Clusters and nodes
    """

    def __init__(self, config, persistence_manager):
        super().__init__(config=config,
                         persistence_manager=persistence_manager)

    def metric_project(self):
        """ get count of all clusters and nodes"""
        total_clusters = self.persistence_manager.find(collection="clusters",
                                                       doc_filter={})
        total_nodes = self.persistence_manager.find(collection="nodes",
                                                    doc_filter={})

        final_metric = [("total_clusters", len(total_clusters)),
                        ("total_nodes", len(total_nodes))]
        return self.format_response(final_metric)

    def metric_event_list(self):
        return self.format_response([])
