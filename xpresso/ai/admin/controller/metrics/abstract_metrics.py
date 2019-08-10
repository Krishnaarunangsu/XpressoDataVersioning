""" Abstract Metrics class"""
from xpresso.ai.admin.controller.persistence.mongopersistencemanager import \
    MongoPersistenceManager
from xpresso.ai.core.logging.xpr_log import XprLogger

__all__ = ["AbstractMetrics"]
__author__ = ["Naveen Sinha"]


class AbstractMetrics:
    """
    It defines basic format for general metrics.
    """

    def __init__(self, config, persistence_manager: MongoPersistenceManager):
        self.config = config
        self.logger = XprLogger()
        self.persistence_manager = persistence_manager

    @staticmethod
    def format_response(tuple_list: list):
        return [{"label": item[0], "data": item[1]} for item in tuple_list]

    @staticmethod
    def find_last_n_unique_item(source_list, n):
        """ Find first n unique item """
        return list(set(source_list))[:n]

