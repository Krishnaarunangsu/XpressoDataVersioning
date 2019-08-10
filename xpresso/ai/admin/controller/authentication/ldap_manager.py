__all__ = ['LdapManager']
__author__ = 'Srijan Sharma'

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
import ldap
import ldap.modlist as modlist


class LdapManager():
    """
    Creates a class to perform LDAP operations i.e. authenticating user etc.
    """
    LDAP_SECTION = 'ldap'
    URL = 'ldap_url'

    def __init__(self):
        self.config = XprConfigParser(XprConfigParser.DEFAULT_CONFIG_PATH)
        self.logger = XprLogger()
        self.adminuser = "admin"
        self.adminpassword = "admin"

    def authenticate(self, username, password):
        """
        Authenticates user using LDAP server

        Args:
        username(str): unique username provided
        password(str):user account password

        Returns:
            bool : return True  if user authenticated successfully,
            else raises corresponding Excecption

        """
        self.logger.info("Authenticating using LDAP")
        ldap_server = self.config[self.LDAP_SECTION][self.URL]

        user_dn = f'cn={username},dc=abzooba,dc=com'
        connect = ldap.initialize(ldap_server)

        try:
            connect.bind_s(user_dn, password)
            self.logger.info(
                "User:{} Succesfully Authenticated".format(username))
            return True
        finally:
            connect.unbind_s()
        return False

    def add(self, username, password):
        """
        Adds a new user

        Args:
        username(str): Name of the user account to be added
        password(str): Password specified for the account

        Returns:
            bool : return True  if user added successfully,
            else raises corresponding excecption

        """
        ldap_server = self.config[self.LDAP_SECTION][self.URL]
        connect = ldap.initialize(ldap_server)

        user_dn = f'cn={self.adminuser},dc=abzooba,dc=com'

        add_dn = f'cn={username},dc=abzooba,dc=com'
        attrs = {}
        attrs['objectclass'] = [b'simpleSecurityObject', b'organizationalRole']
        attrs['cn'] = [str.encode(username)]
        attrs['userPassword'] = [str.encode(password)]
        attrs['description'] = [b'Xpresso User']

        try:
            connect.bind_s(user_dn, self.adminpassword)
            connect.add_s(add_dn, modlist.addModlist(attrs))
            self.logger.info("Successfully added user {}".format(username))
            return True
        except ldap.INVALID_CREDENTIALS as e:
            self.logger.error("Invalid credentials provided : {}".format(e))
            raise e
            return False
        except ldap.LDAPError as e:
            self.logger.error("Error : {}".format(e))
            raise e
            return False
        finally:
            connect.unbind_s()
        return False

    def update_password(self, username, old_password, new_password):
        """
        Updates an already existing user account password
        username(str): Name of the user account to be added
        old_password(str)

        Args:: Already existing password
        new_password(str) : New user password

        Returns:
            bool : return True  if user password updated successfully,
            else raises corresponding Excecption

        """

        ldap_server = self.config[self.LDAP_SECTION][self.URL]
        connect = ldap.initialize(ldap_server)
        user_dn = f'cn={username},dc=abzooba,dc=com'
        try:
            connect.bind_s(user_dn, old_password)
            add_pass = [(ldap.MOD_REPLACE, 'userPassword',
                         [str.encode(new_password)])]
            connect.modify_s(user_dn, add_pass)
            self.logger.info("Successfully updated password for {}".format(
                username))
            return True
        except ldap.LDAPError as e:
            self.logger.error("Error : {}".format(e))
        finally:
            connect.unbind_s()
        return False


if __name__ == "__main__":
    ld = LdapManager()
