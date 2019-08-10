__all__ = ['EnvManager']
__author__ = 'R Krishna Kumar'

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.persistence.mongopersistencemanager import MongoPersistenceManager
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import InvalidEnvironmentException
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import NoClustersPresentException
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import IncorrectDeploymentException
from collections import OrderedDict

config_path = XprConfigParser.DEFAULT_CONFIG_PATH
config = XprConfigParser(config_path)

MONGO_SECTION = 'mongodb'
URL = 'mongo_url'
DB = 'database'
UID = 'mongo_uid'
PWD = 'mongo_pwd'
W = 'w'
MONGOURL = config[MONGO_SECTION][URL]
MONGODB = config[MONGO_SECTION][DB]
MONGOUID = config[MONGO_SECTION][UID]
MONGOPWD = config[MONGO_SECTION][PWD]
MONGOW = config[MONGO_SECTION][W]

ENVIRONMENTS_COLLECTION = "environments"
CLUSTERS_COLLECTION = "clusters"
PROJECT_IDENTIFIER = "project"
ENVIRONMENT_IDENTIFIER = "env"
CLUSTER_IDENTIFIER = "cluster"
NAME_IDENTIFIER = "name"
TARGET_ENVIRONMENT_IDENTIFIER = "target_environment"

PROJECTS_SECTION = "projects"
VALID_ENVIRONMENTS_SUBSECTION = "valid_environments"
VALID_ENVS = config[PROJECTS_SECTION][VALID_ENVIRONMENTS_SUBSECTION]
DEV_ENVIRONMENT = "DEV"


class EnvManager:
    """
    This class manages Xpresso project deployment environments
    Each project can have multiple environments assigned to it
    Each environment is mapped to a cluster on which actual deployment occurs
    """
    logger = XprLogger()

    def allocate_env(self, project, envs):
        """
        Allocates clusters to the environments required for a project
        :param project: name of project
        :param envs: array containing names of (valid) environments required, e.g., "DEV", "QA", etc.
        :return: None (exception if any environment is invalid, or if no cluster is available for allocation)
        """
        # 1. check if environments specified are valid - if not raise InvalidEnvironmentException
        # 2. get list of environments already allocated to project, and do a diff between the allocated list and envs
        # to get a list of new envs required (new_envs)
        #
        # 3. get list of clusters and count of environments allocated to each from database
        # 4. Sort cluster list (least-to-highest)
        # 5. for each item in new_envs
        #    allot item to cluster at top of list (i.e., cluster with least envs allotted)
        #    re-sort cluster list

        # 1. check if environments specified are valid - if not raise InvalidEnvironmentException
        self.logger.debug("Checking environments specified for validity")
        if not envs:
            return

        if not all(env in VALID_ENVS for env in envs):
            raise InvalidEnvironmentException()

        self.logger.debug("Environments specified are valid")


        # 2. get list of environments already allocated to project, and do a diff between the allocated list and envs
        # to get a list of new envs required (new_envs)
        self.logger.debug("Checking on new environments required")
        new_envs = []
        mongo_persistence_manager = MongoPersistenceManager(url=MONGOURL, db=MONGODB, uid=MONGOUID, pwd=MONGOPWD)
        mongo_persistence_manager.connect()
        allocated_envs = mongo_persistence_manager.find(ENVIRONMENTS_COLLECTION, {PROJECT_IDENTIFIER: project})
        for env in envs:
            found = False
            for i in range(len(allocated_envs)):
                if env == allocated_envs[i][ENVIRONMENT_IDENTIFIER]:
                    found = True
                    break
            if not found:
                new_envs.append(env)

        self.logger.debug("Checked on new environments required. New environments are " + str(new_envs))

        # 3. get list of clusters and count of environments allocated to each from database
        self.logger.debug("Getting list of currently allocated environments from database")
        # get a list of currently available clusters
        clusters = mongo_persistence_manager.find(CLUSTERS_COLLECTION, {"activationStatus": True})
        if len(clusters) == 0:
            raise NoClustersPresentException()

        # get a list of environments to calculate count of each cluster
        self.logger.debug("Getting allocated environment count for eqach cluster")
        current_envs = mongo_persistence_manager.find(ENVIRONMENTS_COLLECTION, {})
        self.logger.debug("Got environments from database: " + str(len(current_envs)))
        env_count = {}
        for cluster in clusters:
            cluster_name = cluster["name"]
            env_count[cluster_name] = 0
            for record in current_envs:
                env_cluster_name = record[CLUSTER_IDENTIFIER]
                if cluster_name == env_cluster_name:
                    env_count[cluster_name] = env_count[cluster_name] + 1
        self.logger.debug("Got list of allocated environments: " + str(env_count))

        # 4. Sort cluster list (least-to-highest)
        self.logger.debug("Sorting list of clusters by environments allocated")
        sorted_cluster_count = OrderedDict(sorted(env_count.items(), key=lambda t: t[1]))
        self.logger.debug("Sorted. List: " + str(sorted_cluster_count))

        # 5. for each item in new_envs
        #    allot item to cluster at top of list (i.e., cluster with least envs allotted)
        #    re-sort cluster list
        self.logger.debug("Allocating new environments to clusters")
        for new_env in new_envs:
            cluster_name = list(sorted_cluster_count.keys())[0]
            orig_count = sorted_cluster_count[cluster_name]
            self.add_env(project, new_env, cluster_name)
            sorted_cluster_count[cluster_name] = orig_count + 1
            sorted_cluster_count = OrderedDict(sorted(sorted_cluster_count.items(), key=lambda t: t[1]))
        self.logger.debug("Allocated. New sorted list is " + str(sorted_cluster_count))

        return

    def add_env(self, project, env, cluster):
        """
        Adds a single environment-cluster mappinng to the "environments" collection of the database
        :param project: name of project
        :param env: name of environment
        :param cluster: name of cluster
        :return: None
        """
        self.logger.info(
            "Entering add_env with parameters project %s, env %s, cluster %s" % (
                project, env, cluster))
        # add a record to the database
        mongo_persistence_manager = MongoPersistenceManager(url=MONGOURL, db=MONGODB, uid=MONGOUID, pwd=MONGOPWD)
        mongo_persistence_manager.connect()
        mongo_persistence_manager.insert(ENVIRONMENTS_COLLECTION, {PROJECT_IDENTIFIER: project,
                                        ENVIRONMENT_IDENTIFIER: env, CLUSTER_IDENTIFIER: cluster}, False)
        self.logger.info("Created record for project environment in environments collection")

        return

    def remove_env(self, project, env=None):
        """
        Removes an environment from a project ("DEV" environment CANNOT be removed)

        :param project: name of project
        :param env: name of environment
        :return: None
        """
        self.logger.info(
            "Entering remove_env with parameters project %s, env %s" % (
                project, env))
        # remove record from the database
        mongo_persistence_manager = MongoPersistenceManager(url=MONGOURL, db=MONGODB, uid=MONGOUID, pwd=MONGOPWD)
        mongo_persistence_manager.connect()
        removal_json = {PROJECT_IDENTIFIER: project}
        if env is not None:
            removal_json[ENVIRONMENT_IDENTIFIER] = env
        mongo_persistence_manager.delete(ENVIRONMENTS_COLLECTION, removal_json)
        self.logger.info("Removed record for project environment from environments collection")

        return


    def get_cluster(self, project, env):
        """
        gets the name of the cluster associated with a specific project environment
        :param project: name of project
        :param env: name of environment
        :return: name of cluster
        """
        self.logger.info(
            "Entering get_cluster with parameters project %s, env %s" % (
                project, env))
        # get record from the database
        mongo_persistence_manager = MongoPersistenceManager(url=MONGOURL, db=MONGODB, uid=MONGOUID, pwd=MONGOPWD)
        mongo_persistence_manager.connect()
        results = mongo_persistence_manager.find(ENVIRONMENTS_COLLECTION, {PROJECT_IDENTIFIER: project,
                                        ENVIRONMENT_IDENTIFIER: env})
        self.logger.info("Got record for project environment from environments collection " + str(results))
        if len(results) == 0:
            return None
        else:
            return results[0]["cluster"]


    def validate_deployment_target_env(self, input_project, db_project_info):
        """
        validates if the project has been deployed to all environments lower than the proposed target
        :param input_project: deployment json obtained from user
        :param db_project_info: project info json from database
        :return: raise exception if validation rule violated
        """
        # 1. check that the target environment is valid and present in the project
        self.logger.info("Entered with input_project " + str(input_project) + " and db_project_info "
                         + str(db_project_info))
        project_name = input_project[NAME_IDENTIFIER]
        try:
            target_env = input_project[TARGET_ENVIRONMENT_IDENTIFIER]
        except KeyError:
            raise InvalidEnvironmentException ("Target Environment not specified")

        try:
            project_envs = db_project_info["environments"]
        except KeyError:
            project_envs = []

        if target_env not in project_envs:
            raise InvalidEnvironmentException()
        self.logger.info("project name = " + project_name + ", target env = " + target_env + ", project envs = "
                         + str(project_envs))

        # 2. check if all environments available to the project lower than the target
        # have been deployed earlier
        try:
            deployed_envs = db_project_info["deployedEnvironments"]
        except KeyError:
            deployed_envs = []

        self.logger.info ("Currently deployed env = " + str(deployed_envs))
        self.logger.info("Checking target to see that all lower environments have been deployed")
        for env in VALID_ENVS:
            if env == target_env:
                break
            elif env in project_envs and env not in deployed_envs:
                raise IncorrectDeploymentException()

        return True

    def deactivate_cluster(self, cluster):
        """
        deactivates a cluster and allocates environments on it to other clusters
        :param cluster: name of deactivated cluster
        :return:
        """

        # 1. get a list of environments allocated to the deactivated cluster
        self.logger.info("Deactivating cluster " + cluster)
        mongo_persistence_manager = MongoPersistenceManager(url=MONGOURL, db=MONGODB, uid=MONGOUID, pwd=MONGOPWD)
        mongo_persistence_manager.connect()
        results = mongo_persistence_manager.find(ENVIRONMENTS_COLLECTION, {CLUSTER_IDENTIFIER: cluster})

        self.logger.info("Cluster has " + str(len(results)) + " environments currently allocated - re-allocating these")

        # 2. remove currennt allocations
        mongo_persistence_manager.delete(ENVIRONMENTS_COLLECTION, {CLUSTER_IDENTIFIER: cluster})

        # 3. re-allocate
        for record in results:
            self.logger.info("Reallocating env " + record["env"] + " for project " + record["project"])
            envs = []
            envs.append(record["env"])
            self.allocate_env(record["project"], envs)
        self.logger.info("Re-allocation complete")

