#!/usr/bin/python3.7

from passlib.hash import sha512_crypt

from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.user_management.user import User
from xpresso.ai.admin.controller.nfs.nfs_user_manager import NFSUserManager
from xpresso.ai.admin.controller.utils.xprresponse import XprResponse
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.authentication.ldap_manager import LdapManager


class UserManager:

    CONTROLLER_SECTION = 'controller'
    TOKEN_EXPIRY = 'soft_expiry'
    LOGIN_EXPIRY = 'hard_expiry'
    AUTHENTICATION_TYPE = "authentication_type"

    def __init__(self, persistence_manager):
        self.logger = XprLogger()
        self.config = XprConfigParser()
        self.persistence_manager = persistence_manager
        self.ldapmanager = LdapManager()

    def register_user(self, user_json):
        """
        register a new user in the persistence

        checks if the user already exists and then adds to persistence

        Parameters:
            user_json [json]: json with node information

        Return:
            Success -> 'OK' [str] : returns 'OK' as response
            Failure -> [str] : returns appropriate failure response
        """
        self.logger.debug(f"Entered register_user with {user_json}")
        # create user object
        new_user = User(user_json)
        # run validations
        new_user.validate_mandatory_fields()
        new_user.validate_field_values()
        # valid inputs - exception would have been raised in case of missing /
        # invalid info

        # now, set other fields as required
        # Password should not be saved as plain text in database.
        # Encrypting the password before saving it to database
        self.logger.info("Registering a new user")
        user_pwd = user_json["pwd"]
        new_user.set('pwd', sha512_crypt.encrypt(user_json['pwd']))
        new_user.set('loginStatus', False)
        new_user.set('activationStatus', True)
        new_user.set('nodes', [])
        self.logger.info("adding user to the database")

        self.persistence_manager.insert("users", new_user.data, False)
        self.logger.info("user successfully added to the persistence")

        try:
            self.ldapmanager.add(user_json["uid"], user_pwd)
        except Exception as e:
            self.logger.error("Unable to add user")
            print("unable to add user to ldap server : ", e)
            return XprResponse("failure", None, str(e))

        # NFS User directory changes
        self.logger.info("Setting up NFS for the user")
        nfs_manager = NFSUserManager(config=self.config)
        nfs_manager.setup_user_folder(user=user_json['uid'])
        self.logger.info("NFS set up")

        return XprResponse("success", None, None)

    def modify_user(self, filter_json, changes_json):
        """
            modify_user updates the user info in the persistence

            checks if user is available and then updates
            the info as per changes_json

            Parameters:
                filter_json: filter to find user
                changes_json: json with user changes info

            Return:
                Success -> 'OK' [str] : returns OK if provision_node succeeds
                Failure -> [str] : returns appropriate failure response
        """

        self.logger.debug(f"Modifying user information of {filter_json} to {changes_json}")
        # checks if the user is present in database
        self.logger.info("Checking if the user is present in the database")
        users = self.persistence_manager.find("users", filter_json)
        if not users or len(users) == 0:
            self.logger.error(f"user {filter_json} not found in the database")
            raise UserNotFoundException()

        # checks if the user password is also present in changes_json
        temp_user = User(changes_json)
        temp_user.validate_field_values()
        temp_user.validate_modifiable_fields()

        self.logger.info("updating the user information")
        self.persistence_manager.update("users", filter_json, changes_json)
        return XprResponse('success', '', {})

    def deactivate_user(self, uid):
        """
            Deletes an user and his info from the persistence

            Deletes the user from database

            Parameters:
                uid [str] : uid of the user

            Return:
                returns appropriate output
        """
        uid_json = {"uid": uid}
        # deletes the user from persistence

        del_users = self.persistence_manager.find("users", uid_json)
        if del_users and len(del_users) != 0:
            self.logger.info(f"deactivating the user {uid_json['uid']}")
            if 'activationStatus' in del_users[0] and \
                    del_users[0]['activationStatus']:
                self.persistence_manager.update("users", uid_json,
                                                {"activationStatus": False}
                                                )
                self.logger.info(f"user {uid_json['uid']} successfully deactivated")
                return XprResponse('success', '', {})
            else:
                raise DeactivatedUserException
        else:
            raise UserNotFoundException()

    def get_users(self, filter_json, apply_display_filter=True):
        """
            Calls the persistence with input filters to fetch the list of users.
            After fetching, the users list is filtered before sending
            as output in order to send relevant information only

            Parameters:
                filter_json [json] : json with filter key & value pairs

            Return:
                Success -> [list] : returns list of users
                Failure -> [str] : returns persistence failure response
        """
        self.logger.info("getting all the users in the persistence")
        self.logger.debug(f"filter_json is : {filter_json}")
        users = self.persistence_manager.find("users", filter_json)

        # filter user fields before sending the output
        if apply_display_filter:
            new_json = []
            for user_json in users:
                user = User(user_json)
                user.filter_display_fields()
                new_json.append(user.data)
            users = new_json
        # get users call retrieves whole user info from persistence
        # Filtering the data that needs to be shown as output
        self.logger.debug(f'Output of users sent: {users}')
        return users

    def update_password(self, password_json):
        """
        Updates user password

        Checks the password and updates the password on ldap and database

        :param password_json:
            contains the uid, old password & new password
        :return:
            raises exception in case of error
        """
        # uid is mandatory
        if "uid" not in password_json:
            self.logger.info("uid not provided for update password")
            raise IncompleteUserInfoException("User 'uid' not provided")
        uid_json = {"uid": password_json["uid"]}
        # fetches the user information
        users = self.persistence_manager.find("users", uid_json)
        if not len(users):
            self.logger.info("User not found for updating password")
            raise UserNotFoundException()
        # creates user object
        new_user = User(users[0])
        old_password_hash = users[0]["pwd"]
        old_password = password_json["old_pwd"]
        new_password = password_json["new_pwd"]
        # checks if the old password provided is same as the one saved in db
        if not sha512_crypt.verify(old_password, old_password_hash):
            raise InvalidPasswordException("Current password is incorrect")
        # Current and new password should not be same
        if old_password == new_password:
            raise InvalidPasswordException("Current and new password is same.")
        # checks if the password is valid and secure enough
        new_user.check_password(password_json["new_pwd"])
        # updates the password on ldap server
        self.ldapmanager.update_password(
            password_json["uid"], old_password, new_password
        )
        hashed_pwd = sha512_crypt.encrypt(new_password)
        update_json = {
            "pwd": hashed_pwd
        }
        self.persistence_manager.update(
            "users", uid_json, update_json)
