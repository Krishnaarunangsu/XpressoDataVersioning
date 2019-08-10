""" Base object class for job deployment """
from xpresso.ai.admin.controller.project_management.kubernetes.deployment import \
    Deployment

__all__ = ["JobDeployment"]
__author = ["Naveen Sinha"]


class JobDeployment(Deployment):
    """
    Contains attributes of a job based deployment
    """

    def __init__(self, project_name, component_name):
        super().__init__(project_name, component_name)
        self.type = "job"

        self.job_type = "job"
        self.schedule = ""
        self.commands = []

    def is_base_job(self):
        return self.job_type.lower() == 'job'

    def is_cronjob(self):
        return self.job_type.lower() == 'cronjob'
