#!/usr/bin/python3.7

from passlib.hash import sha512_crypt

from xpresso.ai.admin.controller.db.Utils import Utils
from xpresso.ai.admin.controller.utils import error_codes
from xpresso.ai.admin.controller.nfs.nfs_user_manager import NFSUserManager
from xpresso.ai.admin.controller.utils.userutils \
    import userinfocheck, filteruseroutput, modify_user_check
from xpresso.ai.admin.controller.utils.xprresponse import xprresponse
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class xpruser():
    config_path = XprConfigParser.DEFAULT_CONFIG_PATH

    CONTROLLER_SECTION = 'controller'
    TOKEN_EXPIRY = 'soft_expiry'
    LOGIN_EXPIRY = 'hard_expiry'
    MONGO_SECTION = 'mongodb'
    URL = 'mongo_url'
    DB = 'database'
    UID = 'mongo_uid'
    PWD = 'mongo_pwd'
    W = 'w'

    def __init__(self):
        self.config = XprConfigParser(self.config_path)
        self.db_utils = Utils(url=self.config[self.MONGO_SECTION][self.URL],
                              db=self.config[self.MONGO_SECTION][self.DB],
                              uid=self.config[self.MONGO_SECTION][self.UID],
                              pwd=self.config[self.MONGO_SECTION][self.PWD],
                              w=self.config[self.MONGO_SECTION][self.W])
        self.logger = XprLogger()

    def registeruser(self, user_json):
        """
        register a new user in the db

        checks if the user already exists and then adds to db

        Parameters:
            userjson [json]: json with node information

        Return:
            Success -> 'OK' [str] : returns 'OK' as response
            Failure -> [str] : returns appropriate failure response
        """
        self.logger.debug(f"user info provided is {user_json}")
        info_check = userinfocheck(user_json)
        # user info_check checks if the user_json has sufficient info
        if info_check == -1:
            errcode = error_codes.incomplete_user_information
            self.logger.error("Insufficient information to create a new user")
            return xprresponse('failure', errcode, {})
        elif info_check == 0:
            errcode = error_codes.incorrect_primaryRole
            self.logger.error("Incorrect primaryRole has been provided")
            return xprresponse('failure', errcode, {})

        # Password should not be saved as plain text in db.
        # Encrypting the password before saving it to db
        password = sha512_crypt.encrypt(user_json['pwd'])
        user_json['pwd'] = password
        # checks if the user is already present in the db
        self.logger.info("Registering a new user")
        uid_json = {'uid': user_json['uid']}
        self.logger.info("Checking the db if user is already present")
        user = self.db_utils.find("users", uid_json)
        if len(user) != 0:
            errcode = error_codes.user_exists
            return xprresponse('failure', errcode, {})

        user_json['loginStatus'] = False
        user_json['activationStatus'] = True
        user_json['nodes'] = []
        self.logger.info("adding user to the db")
        add_user = self.db_utils.insert("users", user_json, False)
        if add_user == -1:
            errcode = error_codes.username_already_exists
            self.logger.error("username already exists in the db")
            return xprresponse('failure', errcode, {})

        self.logger.info("user successfully added to the db")

        # NFS User directory changes
        nfs_manager = NFSUserManager(config=self.config)
        nfs_manager.setup_user_folder(user=user_json['uid'])
        return xprresponse(
            'success', '', {}
        )

    def modifyuser(self, token, changesjson):
        """
            modify_user updates the user info in the db

            checks if user is available and then updates
            the info as per changesjson

            Parameters:
                uid [str]: uid of the user
                changesjson [json] : json with user changes info

            Return:
                Success -> 'OK' [str] : returns OK if provision_node succeeds
                Failure -> [str] : returns appropriate failure response
        """
        check = modify_user_check(changesjson)
        if check != 200:
            return xprresponse('failure', check, {})

        uidjson = {"uid": changesjson['uid']}
        self.logger.info(f"Modifying user information of {uidjson}")
        self.logger.debug(f"Info provided to be modified is {changesjson}")
        # checks if the user is present in db
        self.logger.info("Checking if the user is present in the db")
        user = self.db_utils.find("users", uidjson)
        if len(user) == 0:
            errcode = error_codes.user_not_found
            self.logger.error(f"user {uidjson['uid']} not found in the db")
            return xprresponse('failure', errcode, {})

        self.logger.info("updating the user information")
        updateuser = self.db_utils.update("users", uidjson, changesjson)
        return xprresponse('success', '', {})

    def deactivateuser(self, uid):
        """
            Deletes an user and his info from the db

            Deletes the user from database

            Parameters:
                uid [str] : uid of the user

            Return:
                returns appropriate output
        """
        uidjson = {"uid": uid}
        # deletes the user from db

        deluser = self.db_utils.find("users", uidjson)
        if len(deluser) != 0:
            self.logger.info(f"deactivating the user {uidjson['uid']}")
            if 'activationStatus' in deluser[0] and \
                deluser[0]['activationStatus']:
                self.db_utils.update("users", uidjson,
                                     {"activationStatus": False}
                                     )
                self.logger.info(f"user {uidjson['uid']} successfully deleted")
                return xprresponse('success', '', {})
            else:
                errcode = error_codes.user_already_deactivated
                return xprresponse('failure', errcode, {})
        else:
            errcode = error_codes.user_not_found
            self.logger.info("user not found")
            return xprresponse('failure', errcode, {})

    def getusers(self, filterjson):
        """
            Calls the db with input filters to fetch the list of users.
            After fetching, the users list is filtered before sending
            as output in order to send relevant information only

            Parameters:
                filterjson [json] : json with filter key & value pairs

            Return:
                Success -> [list] : returns list of users
                Failure -> [str] : returns db failure response
        """
        self.logger.info("getting all the users in the db")
        self.logger.debug(f"filterjson is : {filterjson}")
        users = self.db_utils.find("users", filterjson)
        # get users call retrieves whole user info from db
        # Filtering the data that needs to be shown as output
        self.logger.info("filtering the users before sending output")
        users = filteruseroutput(users)
        self.logger.debug(f'Output of users sent: {users}')
        return xprresponse('success', '', users)
