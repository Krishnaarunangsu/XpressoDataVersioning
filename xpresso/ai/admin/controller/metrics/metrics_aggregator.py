""" Aggregates Different metrics"""
from xpresso.ai.admin.controller.metrics.abstract_metrics import AbstractMetrics
from xpresso.ai.admin.controller.metrics.cluster_metrics import ClusterMetrics
from xpresso.ai.admin.controller.metrics.project_metrics import ProjectMetrics
from xpresso.ai.admin.controller.metrics.user_metrics import UserMetrics
from xpresso.ai.admin.controller.persistence.mongopersistencemanager import \
    MongoPersistenceManager
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser

__all__ = ["MetricsAggregator"]
__author__ = ["Naveen Sinha"]


class MetricsAggregator:
    """ Tracks the list all available metrics.
    Stores the result from these metrics in a dictionary and return
    """

    MONGO_SECTION = 'mongodb'
    DB_URL = 'mongo_url'
    DB_NAME = 'database'
    DB_UID = 'mongo_uid'
    DB_PWD = 'mongo_pwd'
    W = 'w'

    def __init__(self, config_path):
        self.config = XprConfigParser(config_file_path=config_path)
        self.logger = XprLogger()
        self.metrics_list = None
        self.persistence_manager = None

    def initialize(self, db_type=MONGO_SECTION, db_url=DB_URL, db_name=DB_NAME,
                   db_uid=DB_UID, db_pwd=DB_PWD, db_w=W):
        self.persistence_manager = MongoPersistenceManager(
            url=self.config[db_type][db_url],
            db=self.config[db_type][db_name],
            uid=self.config[db_type][db_uid],
            pwd=self.config[db_type][db_pwd],
            w=self.config[db_type][db_w])

        self.metrics_list = [UserMetrics, AbstractMetrics,
                             ClusterMetrics, ProjectMetrics]

    def get_all_metrics(self):
        """ Iterate through all available metrics and get the required
        metrics.
         For all metrics class, it picks all the method which starts with metric_
         """
        self.logger.info("Aggregating all the metrics")
        aggregated_metrics = []
        for metrics_class in self.metrics_list:
            metric_obj = metrics_class(
                config=self.config,
                persistence_manager=self.persistence_manager)
            for metric_name, metric_func in metrics_class.__dict__.items():
                if not metric_name.startswith("metric_"):
                    continue
                aggregated_metrics.extend(metric_func(metric_obj))
        self.logger.info("Metric aggregration compleed")
        return aggregated_metrics


if __name__ == "__main__":
    metric_a = MetricsAggregator(config_path="config/common.json")
    metric_a.initialize()
    print(metric_a.get_all_metrics())
