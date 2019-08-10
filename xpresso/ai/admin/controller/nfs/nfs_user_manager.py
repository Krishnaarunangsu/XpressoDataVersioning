"""  Sets up the user directory and the permissions in the NFS Storage """

__all__ = ['NFSUserManager']
__author__ = 'Naveen Sinha'

import os

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class NFSUserManager:
    """
    For any new or existing project, this class setups the required folder
    structure and user permission for files and folder

    Args:
        config(XPRConfigParser): configuration file
    """

    KUBERNETES_FOLDER = 'k8'
    PODS_FOLDER = 'pods'
    CONFIG_FOLDER = 'config'
    SHARED_FOLDER = 'shared'
    USER_FOLDER = 'users'

    ADMIN_GID = 1000

    def __init__(self, config: XprConfigParser):
        self.config = config["packages_setup"]
        self.logger = XprLogger()

    def setup_project_folder(self, project: str, project_gid: int = ADMIN_GID):
        """
        Create folder within the nfs drive which will be shared across
        multiple developers, VM and deployment VMs.

        Assumption: It assumes the nfs directory to be in a specific structure.
        It might throw an exception if it not.

        Args:
            project(str): project name
            project_gid(int): Linux Group ID of the project

        Returns:
            bool: True, if success, Otherwise raise an OSError exception
        """

        kubernetes_folder = os.path.join(self.config['nfs']['mount_location'],
                                         self.KUBERNETES_FOLDER)

        self.logger.debug("Creating root folders in the NFS")
        try:
            project_folder = self.create_directory(kubernetes_folder, project,
                                                   project_gid, project_gid)
            self.logger.debug("Root folder created")
        except OSError as e:
            self.logger.error("Root creation folder failed")
            raise OSError(e)

        self.logger.debug("Creating sub files and folder")
        try:
            self.create_directory(project_folder, self.PODS_FOLDER,
                                  project_gid, project_gid)
            self.create_directory(project_folder, self.CONFIG_FOLDER,
                                  project_gid, project_gid)
            self.create_directory(project_folder, self.SHARED_FOLDER,
                                  project_gid, project_gid)
            self.logger.debug("Root folder created")
        except OSError as e:
            self.logger.error("Sub directory creation failed")
            raise OSError(e)
        return True

    def create_directory(self, parent_folder, project, user_id, project_gid):
        """
        Create directory and give necessary permissions
        Args:
            parent_folder: Name of the parent folder under which project is
                           created
            project: Name of the project
            project_gid: linux gid for the project
            user_id: linux gid of the user

        Returns:
            str: path of created directory
        """
        project_folder = os.path.join(parent_folder, project)
        try:
            os.makedirs(project_folder, mode=0o777, exist_ok=False)
            os.chown(path=project_folder, uid=user_id, gid=project_gid)
        except PermissionError:
            self.logger.error("can not create new directory")
        return project_folder

    def setup_user_folder(self, user: str, user_uid: int = ADMIN_GID):
        """
        Create user folder within the nfs drive which will be shared across
        multiple developers, VM and deployment VMs.

        Args:
            user(str): username
            user_uid(int): Linux User ID of the project

        Returns:
            bool: True, if success, Otherwise raise an OSError exception
        """
        users_folder = os.path.join(self.config['nfs']['mount_location'],
                                    self.USER_FOLDER)
        if not os.path.exists(users_folder):
            users_folder = self.create_directory(
                self.config['nfs']['mount_location'],
                self.USER_FOLDER,
                self.ADMIN_GID,
                self.ADMIN_GID)

        self.logger.debug("Creating user directory")
        try:
            user_folder = os.path.join(users_folder, user)
            if not os.path.exists(user_folder):
                self.create_directory(users_folder, user,
                                      user_uid, user_uid)
            self.logger.debug("User folder created")
        except OSError as e:
            self.logger.error("Sub directory creation failed")
            raise OSError(e)

    def setup_user_per_project_folder(self, user: str, project: str,
                                      user_uid: int = ADMIN_GID,
                                      project_gid: int = ADMIN_GID):
        """
        Create user folder within the nfs drive which will be shared across
        multiple developers, VM and deployment VMs.

        Args:
            user(str): username
            user_uid(int): Linux User ID of the project
            project(str): project name
            project_gid(int): Linux Group ID of the project

        Returns:
            bool: True, if success, Otherwise raise an OSError exception
        """

        kubernetes_folder = os.path.join(self.config['nfs']['mount_location'],
                                         self.KUBERNETES_FOLDER)

        users_folder = os.path.join(kubernetes_folder, self.USER_FOLDER)
        if not os.path.exists(users_folder):
            users_folder = self.create_directory(kubernetes_folder,
                                                 self.USER_FOLDER,
                                                 self.ADMIN_GID,
                                                 self.ADMIN_GID)

        self.logger.debug("Creating user directory")
        try:
            user_folder = os.path.join(users_folder, user)
            if not os.path.exists(user_folder):
                user_folder = self.create_directory(users_folder, user,
                                                    user_uid, user_uid)

            self.create_directory(user_folder, project, user_uid, project_gid)
            self.logger.debug("User folder created")
        except OSError as e:
            self.logger.error("Sub directory creation failed")
            raise OSError(e)
