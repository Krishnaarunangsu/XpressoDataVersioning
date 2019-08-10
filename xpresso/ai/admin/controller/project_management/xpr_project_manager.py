from xpresso.ai.admin.controller.project_management.local_project_manager \
    import setup_project, modify_project_locally
from xpresso.ai.admin.controller.utils.xprresponse import XprResponse
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.project_management.project import Project
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.env_management.env_manager import EnvManager


class XprProjectManager:

    LINUX_UID_KEY = "linux_uid"
    MIN_VALID_LINUX_UID = 1001

    def __init__(self, persistence_manager):
        self.logger = XprLogger()
        self.persistence_manager = persistence_manager

    def create_project(self, project_json: dict) -> object:
        """
        creates a new project in the persistence and bitbucket

        Creates a new project and repo on bitbucket.
        Then setup necessary nfs mount. Then adds the
        project json to persistence.

        Parameters:
            project_json: json with project information

        Return:
            returns xprresponse object
        """
        # checks if the project_json has the complete info required
        self.logger.info("checking the provided project information")

        # Updating next linux uid
        project_json[self.LINUX_UID_KEY] = self.get_next_linux_uid()
        new_project = Project(project_json)
        new_project.project_info_check(self.persistence_manager)
        new_project.complete_project_info()
        self.logger.debug(f"Updated project info is: {new_project.data}")

        self.logger.info("calling setup_project to complete the setup")
        setup = setup_project(new_project.data)
        setup_code = setup['status']
        self.logger.info(f"setup_project status code is {setup_code}")
        if setup_code != 200:
            self.logger.error("Project setup failed")
            return XprResponse('failure', setup_code,
                               {"message": "Project setup failed"})
        self.logger.info("project setup completed")
        self.logger.info("Adding the project with complete info to persistence")
        self.persistence_manager.insert(
            "projects", setup['project_json'], False
        )
        self.logger.info("project successfully added to persistence")

        # allocate required environments
        self.logger.info("Allocating project environments")
        env_manager = EnvManager()
        env_manager.allocate_env(project_json["name"], project_json["environments"])
        self.logger.info("Allocated project environments")

        new_project.filter_display_fields()
        return XprResponse("success", None, new_project.data)

    def get_next_linux_uid(self):
        """
        Checks the database and finds the next linux uid which needs to be
        assigned to a project
        Returns:
            A valid UID
        """
        all_projects = self.persistence_manager.find("projects", {})
        new_linux_uid = max([self.MIN_VALID_LINUX_UID] +
                            [project[self.LINUX_UID_KEY]
                             for project in all_projects
                             if self.LINUX_UID_KEY in project]) + 1
        return new_linux_uid

    def get_projects(self, filter_json: dict,
                     apply_display_filter=True) -> object:
        """
            Calls the persistence with input filters to fetch the list of projects.

            Parameters:
                filter_json [json] : json with filter key & value pairs

            Return:
                returns a xprresponse object
        """
        self.logger.info("retrieving the list of projects from persistence")
        projects = self.persistence_manager.find("projects", filter_json)
        self.logger.info("calling filter_project_output")
        self.logger.debug(f"projects are: {projects}")
        if apply_display_filter:
            filtered_projects = []
            for project_json in projects:
                temp_project = Project(project_json)
                temp_project.filter_display_fields()
                filtered_projects.append(temp_project.data)
            projects = filtered_projects
        self.logger.debug(f"\n Filtered projects are: {projects}")
        # get users call retrieves whole user info from persistence
        # Filtering the data that needs to be shown as output
        return projects

    def modify_project(self, changes_json: dict):
        """
            Modifies a project in persistence and on bitbucket

            Parameters:
                changes_json: project information that needs to be modified

            Return:
                returns a xprresponse object
        """
        if 'name' not in changes_json:
            raise IncompleteProjectInfoException(
                "Project name needs to be provided for modify_project"
            )

        uid_json = {'name': changes_json['name']}
        self.logger.info("checking if the project is already present")
        projects = self.persistence_manager.find("projects", uid_json)
        if len(projects) == 0:
            self.logger.error("cannot modify a project which doesn't exist.")
            raise NoProjectException("Cannot Modify unregistered project")
        self.logger.info("calling modify_info_check to validate the info")
        new_project = Project(projects[0])
        new_project.modify_info_check(changes_json, self.persistence_manager)
        self.logger.info("modify_project_locally has been called")
        modify_status = modify_project_locally(projects[0], changes_json)
        if modify_status != 200:
            self.logger.error("modify_project_locally failed")
            XprResponse('failure', modify_status,
                        {"message": "Modify project failed"})

        self.logger.info(
            "project info is being modified before updating @persistence")
        update_json = new_project.modify_project_info(changes_json)
        self.persistence_manager.update("projects", uid_json, update_json)
        self.logger.info("Project modified successfully")

        # allocate required environments
        self.logger.info("Allocating project environments")
        env_manager = EnvManager()
        env_manager.allocate_env(changes_json["name"], changes_json["environments"])
        self.logger.info("Allocated project environments")

    def deactivate_project(self, uid_json: dict):
        """
            Deactivates a project. updates the appropriate flags in persistence

            Parameters:
                uid [str] : uid of the project

            Return:
                returns xprresponse object
        """
        # deletes the project from persistence
        self.logger.info("Checking if the project actually exists")
        projects = self.persistence_manager.find("projects", uid_json)
        if len(projects) == 0:
            raise NoProjectException()
        elif 'activationStatus' not in projects[0]:
            projects[0]['activationStatus'] = True

        if projects[0]['currentlyDeployed']:
            raise ProjectDeactivationException(
                "Project Currently deployed. Undeploy first."
            )
        elif not projects[0]['activationStatus']:
            raise ProjectDeactivationException(
                "Project already deactivated"
            )

        active_flag_json = {"activationStatus": False}
        self.persistence_manager.update("projects", uid_json, active_flag_json)
        # update_id = self.db_utils.delete("projects", uid_json)

        # remove allocated environments
        self.logger.info("Removing project environments")
        env_manager = EnvManager()
        env_manager.remove_env(uid_json["name"])
        self.logger.info("Removed project environments")
