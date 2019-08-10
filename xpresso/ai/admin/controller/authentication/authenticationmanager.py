
__all__ = ['AuthenticationManager']
__author__ = 'Sahil Malav'

from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.authentication.authenticationcontext import AuthenticationContext
from xpresso.ai.admin.controller.utils.xprresponse import XprResponse
from xpresso.ai.core.logging.xpr_log import XprLogger
from passlib.hash import sha512_crypt
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.authentication.tokeninfo import TokenInfo
from xpresso.ai.admin.controller.user_management.user import User
from xpresso.ai.admin.controller.user_management.usermanager import UserManager
from xpresso.ai.admin.controller.authentication.ldap_manager import LdapManager
import ldap

# temporary
levels = {'Dev': 0, 'PM': 1, 'DH': 1, 'Admin': 2, 'Su': 3}


class AuthenticationManager:
    """
    This class provides user authentication functionality for Xpresso Controller
    """

    AUTHENTICATION_TYPE = "authentication_type"

    def __init__(self, persistence_manager):
        self.persistence_manager = persistence_manager
        self.logger = XprLogger()
        self.ldapmanager = LdapManager()
        config_path = XprConfigParser.DEFAULT_CONFIG_PATH
        self.config = XprConfigParser(config_path)

    def login(self, credentials):
        """
        Authentication method for user login.
        Args:
            credentials: object containing uid and pwd

        Returns: dictionary object consisting of access token

        """
        try:
            # validate data
            authentication_context = AuthenticationContext(credentials)
            uid = authentication_context.get('uid')
            self.logger.debug('entering login method '
                              'with uid {}'.format(uid))
            check = self.validate_credentials(authentication_context)
            if check == error_codes.already_logged_in:
                self.logger.debug('Relogin request from {}.'.format(uid))
                relogin = True
            else:
                relogin = False
            self.logger.debug('Providing new token to {}.'.format(uid))
            token_info = self.generate_token()
            self.save_token({"uid": uid}, token_info)
            credentials = {'access_token': token_info.token, 'relogin': relogin}
            response = XprResponse('success', None, credentials)
            self.logger.debug('Login successful. Exiting.')
            return response

        except AuthenticationFailedException as e:
            self.logger.error(
                'Authentication failed for user {}'.format(uid))
            return XprResponse("failure", e.error_code,
                               {"message": "Authentication failed"})

    def validate_credentials(self, authentication_context):

        uid = authentication_context.get('uid')
        pwd = authentication_context.get('pwd')
        self.logger.debug('validating credentials for {}'.format(uid))

        users = UserManager(
            self.persistence_manager).get_users({"uid": uid}, False)
        print('validate_credentials: {}'.format(users))
        if not users:
            self.logger.error("User {} not found".format(uid))
            raise UserNotFoundException(f"User {uid} not found")
        hashed_pwd = users[0]['pwd']
        if not self.authenticate_password(uid, pwd, hashed_pwd):
            self.logger.error("Wrong password entered by {}".format(uid))
            raise WrongPasswordException("Wrong password")
        elif not users[0]["activationStatus"]:
            self.logger.error("User {} has been deactivated".format(uid))
            raise DeactivatedUserException("This user is deactivated. "
                                           "Please reactivate first")
        elif users[0]['loginStatus']:
            self.logger.debug("User '{}' is already logged in.".format(uid))
            return error_codes.already_logged_in
        elif not users[0]['activationStatus']:
            raise DeactivatedUserException(
                "This user is deactivated. Please reactivate first"
                )
        print('credentials validated for {}'.format(uid))
        return True

    def authenticate_password(self, uid, pwd, hashed_pwd):
        """
        Uses differnet authentication method to check if credentials are
        valid
        """
        if self.config[self.AUTHENTICATION_TYPE] == "ldap":
            try:
                self.ldapmanager.authenticate(uid, pwd)
                return True
            except ldap.INVALID_CREDENTIALS as e:
                self.logger.error("Wrong password entered by {}".format(uid))
                raise WrongPasswordException(str(e))
            except ldap.LDAPException as e:
                self.logger.error("Invalid credentials {}".format(uid))
                raise AuthenticationFailedException(str(e))
        elif self.config[self.AUTHENTICATION_TYPE] == "mongodb":
            if not sha512_crypt.verify(pwd, hashed_pwd):
                self.logger.error("Wrong password entered by {}".format(uid))
                raise WrongPasswordException("Wrong Password")
            return True
        return False

    def generate_token(self):
        self.logger.debug('Generating token')
        token_info = TokenInfo(None)
        token_info.generate()
        return token_info

    def save_token(self, search_filter, token_info):
        # print(search_filter)
        UserManager(self.persistence_manager).modify_user(
            search_filter,
            {"token": token_info.token, "loginStatus": token_info.login_status,
             "loginExpiry": token_info.login_expiry,
             "tokenExpiry": token_info.token_expiry})

    def logout(self, token):
        """
        Authentication method for user logout
        Args:
            token: access token

        Returns: Deletion status (True/False)

        """
        self.logger.info('entering logout method')
        status = self.delete_token(token)
        self.logger.debug(
            'exiting logout method with status {}'.format(status))
        return XprResponse("success", None, {})

    def delete_token(self, token):
        # delete token from database & change status
        token_info = TokenInfo(None)
        self.save_token({"token": token}, token_info)
        return True

    def modify_user_access(self, token, uid):
        users = UserManager(self.persistence_manager).get_users(
            {"token": token}, False)
        if users and len(users) > 0:
            if (uid == users[0]['uid']) or (users[0]['primaryRole'] == 'Admin'):
                return True
            else:
                return False
        else:
            return False

    def validate_token(self, token, access_level):
        users = UserManager(self.persistence_manager).get_users(
            {"token": token}, False)
        if not users:
            self.logger.debug('Tokens do not match. Re-login needed.')
            raise IncorrectTokenException
        else:
            user = User(users[0])
            token_info = user.get_token_info()
            if token_info.has_expired():
                self.logger.debug('Token expired for {}.'
                                  'Logging out.'.format(user.get('uid')))
                token_info = TokenInfo(None)
                self.save_token({"token": token}, token_info)
                raise ExpiredTokenException
            elif not self.check_access(user.get('primaryRole'), access_level):
                self.logger.debug('User access check failed! Exiting.')
                raise PermissionDeniedException("Permission Denied")
        self.logger.info('revalidating token')
        self.revalidate_token(token_info)
        self.logger.info('exiting validate_token method')
        return True

    def validate_build_deploy_token(self, token, project):
        users = UserManager(self.persistence_manager).get_users(
            {"token": token}, False)
        if not users or len(users) == 0:
            self.logger.debug('Tokens do not match. Re-login needed.')
            raise IncorrectTokenException
        else:
            user = User(users[0])
            token_info = user.get_token_info()
            if token_info.has_expired():
                self.logger.debug('Token expired for {}.'
                                  'Logging out.'.format(users[0]['uid']))
                token_info = TokenInfo(None)
                self.save_token({"token": token}, token_info)
                raise ExpiredTokenException

        if "name" not in project:
            raise ProjectNotFoundException("Project name is empty")

        search_filter = {"name": project["name"]}
        project_info = self.persistence_manager.find('projects', search_filter)
        if not project_info:
            self.logger.error('No such project found.')
            raise ProjectNotFoundException("No such Project Found")
        if users[0]['uid'] != project_info[0]['owner']['uid'] and \
                levels[users[0]['primaryRole']] < levels['Admin']:
            self.logger.debug('User access check failed! Exiting.')
            raise PermissionDeniedException
        self.logger.info('revalidating token')
        self.revalidate_token(token_info)
        self.logger.info('exiting validate_token method')
        return project_info

    def revalidate_token(self, token_info):
        token_info.revalidate()
        self.save_token({"token": token_info.token}, token_info)
        self.logger.info('token revalidated.')

    def check_access(self, primary_role, access_level):
        self.logger.debug('Checking access')
        return levels[access_level] <= levels[primary_role]
