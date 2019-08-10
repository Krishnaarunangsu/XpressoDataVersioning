from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.xprobject import XprObject
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *

logger = XprLogger()


class AuthenticationContext (XprObject):

    def __init__(self, credentials):
        super().__init__(credentials)
        if 'uid' not in credentials or not len(credentials['uid']):
            raise InvalidUserIDException
        elif 'pwd' not in credentials or not len(credentials['pwd']):
            raise InvalidPasswordException
