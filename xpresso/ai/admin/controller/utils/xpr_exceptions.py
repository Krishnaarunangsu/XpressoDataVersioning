class XprExceptions(Exception):
    """ General Xpresso exception occurred """

    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        """
        gets a string representation of this exception
        :return: string representation of exception
        """
        return self.message


class UserNotFoundException(XprExceptions):
    """
    class for exception thrown when the requested user is not found in the db.
    """


class LogoutFailedException(XprExceptions):
    """
    class for exception thrown when logout request fails.
    """


class AuthenticationFailedException(XprExceptions):
    """
    class for exception thrown when the authentication fails
    """


class RevalidationFailedException(XprExceptions):
    """
    class for exception thrown when the authentication fails
    """


class ClusterRequestFailedException(XprExceptions):
    """
    class for exception thrown when the requested cluster is not found in the db.
    """


class BuildRequestFailedException(XprExceptions):
    """
    class for exception thrown when project build fails.
    """


class DeployRequestFailedException(XprExceptions):
    """
    class for exception thrown when project deployment fails.
    """


class HTTPRequestFailedException(XprExceptions):
    """
    class for exception thrown when HTTP request fails
    """


class HTTPInvalidRequestException(XprExceptions):
    """
    class for exception thrown when HTTP request is invalid
    """
