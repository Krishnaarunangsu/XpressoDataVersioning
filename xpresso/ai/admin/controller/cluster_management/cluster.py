from xpresso.ai.admin.controller.xprobject import XprObject
from xpresso.ai.core.logging.xpr_log import XprLogger


class Cluster(XprObject):
    """
    This class represents a cluster
    """

    def __init__(self, cluster=None):
        self.logger = XprLogger()
        self.logger.debug("Cluster constructor called with {}".format(cluster))
        super().__init__(cluster)
        self.mandatory_fields = ['name']

