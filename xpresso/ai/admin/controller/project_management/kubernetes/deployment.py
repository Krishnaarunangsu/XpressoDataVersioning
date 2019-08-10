""" Base object class for  deployment """

__all__ = ["Deployment"]
__author = ["Naveen Sinha"]


class Deployment:
    """
    Contains attributes of a deployment


    Args:
        project_name: project to be deployed
        component_name: component for which this yaml is generated

    Attributes:
        docker_image: docker image name
        replicas: number of replicas that component requires for deployment
        environment: environment for deployment
        persistence: persitence for deployment
    """

    SERVICE = "service"
    JOB = "job"
    DATABASE = "database"

    def __init__(self, project_name, component_name):
        self.project_name = project_name
        self.component_name = component_name
        self.type = self.SERVICE

        self.docker_image = ""
        self.replicas = 1
        self.environment = []
        self.persistence = []
        self.volume_size = "2Gi"
        self.volume_mount_path = "/mnt"
        self.build_version = 1
        self.project_linux_uid = 1001

        self.is_external = False

    def is_service(self):
        return bool(self.type == self.SERVICE)

    def is_database(self):
        return bool(self.type == self.DATABASE)

    def is_job(self):
        return bool(self.type == self.JOB)

    def need_persistence(self):
        return bool(self.persistence)

    def is_external_required(self):
        return self.is_external
