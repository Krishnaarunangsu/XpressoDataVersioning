""" Base object class for service deployment """
from xpresso.ai.admin.controller.project_management.kubernetes.deployment import \
    Deployment

__all__ = ["ServiceDeployment"]
__author = ["Naveen Sinha"]


class ServiceDeployment(Deployment):
    """
    Contains attributes of a service based deployment
    """

    def __init__(self, project_name, component_name):
        super().__init__(project_name, component_name)
        self.type = "service"

        self.ports = []
        self.master_node = ""

