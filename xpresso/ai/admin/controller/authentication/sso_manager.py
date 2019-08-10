""" Class description for SSOManager"""

__all__ = ['SSOManager']
__author__ = 'Sahil Malav'

import secrets

from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    IncorrectTokenException
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class SSOManager:
    """
    This class provides user authentication functionality for Xpresso Controller
    """

    def __init__(self, persistence_manager):
        self.persistence_manager = persistence_manager
        self.logger = XprLogger()

        config_path = XprConfigParser.DEFAULT_CONFIG_PATH
        self.config = XprConfigParser(config_path)

    def validate_token(self, validation_token):
        """
        Validate if the current token is valid
        Returns:
        """
        self.logger.info("Validating token")
        token_data = self.persistence_manager.find('sso_tokens',
                                                   {"validation_token":
                                                        validation_token})
        if not token_data:
            self.logger.error("Token does not exist")
            raise IncorrectTokenException("Token does not exist")
        self.logger.info("Token validated")
        return token_data[0]["login_token"]

    @staticmethod
    def generate_token():
        """
        generate a new token
        """
        return secrets.token_hex(8)

    def update_token(self, validation_token, login_token):
        """
        Update validation and login token for match
        Args:
            validation_token: sso  login validation token
            login_token: login token
        """
        self.persistence_manager.insert("sso_tokens",
                                        {"validation_token": validation_token,
                                         "login_token": login_token},
                                        duplicate_ok=True)
