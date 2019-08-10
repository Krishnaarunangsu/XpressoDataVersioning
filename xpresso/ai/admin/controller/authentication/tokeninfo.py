import secrets
from time import time
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.xprobject import XprObject

CONTROLLER_SECTION = 'controller'
TOKEN_EXPIRY = 'soft_expiry'
LOGIN_EXPIRY = 'hard_expiry'


class TokenInfo(object):
    """
    This class represents an access token
    """

    def __init__(self, token):
        self.logger = XprLogger()

        config_path = XprConfigParser.DEFAULT_CONFIG_PATH
        self.config = XprConfigParser(config_path)
        self.token = token
        self.token_expiry = None
        self.login_expiry = None
        self.login_status = False

    def generate(self):
        self.logger.debug('Generating token')
        self.token = secrets.token_hex(32)
        self.token_expiry = \
            time() + int(self.config[CONTROLLER_SECTION][TOKEN_EXPIRY])
        self.login_expiry = \
            time() + int(self.config[CONTROLLER_SECTION][LOGIN_EXPIRY])
        self.login_status = True

    def has_expired(self):
        if (self.token_expiry < time() or
                self.login_expiry < time()):
            return True
        else:
            return False

    def revalidate(self):
        self.token_expiry = \
            time() + int(self.config[CONTROLLER_SECTION][TOKEN_EXPIRY])

