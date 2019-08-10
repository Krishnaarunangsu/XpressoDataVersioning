import re

from xpresso.ai.admin.controller.xprobject import XprObject
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.authentication.tokeninfo import TokenInfo
from xpresso.ai.admin.controller.exceptions.xpr_exceptions \
    import PasswordStrengthException


class User(XprObject):
    """
    This class represents a User
    """
    def __init__(self, user_json=None):
        self.logger = XprLogger()
        """
        Constructor:
        """
        self.logger.debug(f"User constructor called with {user_json}")
        super().__init__(user_json)
        self.logger.info(f"user info : {self.data}")
        # These are mandatory fields that needs to be provided in user_json
        self.mandatory_fields = [
            "uid", "pwd", "firstName", "lastName",
            "email", "primaryRole"
        ]

        # primaryRole of a user has to one of these
        self.valid_values = {"primaryRole": ["Dev", "PM", "DH", "Admin", "Su"]}

        # fields that cannot be modified
        self.unmodifiable_fields = ["pwd"]

        # fields that should be displayed in the output
        self.display_fields = ["uid", "firstName", "lastName", "email",
                               "primaryRole", "nodes", "activationStatus"]
        self.logger.debug("User constructed successfully")

    def get_token_info(self):
        token_info = TokenInfo(self.data['token'])
        token_info.token = self.get("token")
        token_info.token_expiry = self.get("tokenExpiry")
        token_info.login_expiry = self.get("loginExpiry")
        return token_info

    @staticmethod
    def check_password(password):
        if len(password) < 6:
            raise PasswordStrengthException("Password is too short")
        reg_exp = "^(((?=.*[a-z])(?=.*[A-Z]))|((?=.*[a-z])(?=.*[0-9]))|((?=.*[A-Z])(?=.*[0-9])))(?=.{6,})"
        match = re.search(reg_exp, password)
        if not match:
            raise PasswordStrengthException("Password is weak. Choose a strong password")