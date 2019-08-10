""" Connects to remote ssh server"""

__all__ = ['SSHClient']
__author__ = 'Naveen Sinha'

import paramiko

from xpresso.ai.core.logging.xpr_log import XprLogger


class SSHClient:
    """
    It connects to the ssh service using username/passs or sshkeys.
    It is mainly used to run packages on remote ssl based servers
    """

    def __init__(self, hostname, username=None, password=None,
                 private_key=None, passphrase=None, port=22):

        self.logger = XprLogger()

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.hostname = hostname
        self.username = username
        self.password = password
        self.private_key = private_key
        self.passphrase = passphrase
        self.port = port

    def connect(self):
        """
        Create SSH connection object
        """
        try:
            if self.private_key:
                self.ssh_client.connect(hostname=self.hostname,
                                        port=self.port,
                                        pkey=self.private_key,
                                        passphrase=self.passphrase)

            elif self.username:
                self.ssh_client.connect(hostname=self.hostname,
                                        port=self.port,
                                        username=self.username,
                                        password=self.password)
            else:
                self.logger.error("Empty credentials provided")

        except(paramiko.BadHostKeyException, paramiko.SSHException,
               paramiko.AuthenticationException) as e:
            self.logger.error("Could not connect to Remote Server {}"
                              .format(e))

    def execute(self, command, stream=True):
        """
        Execute a string packages in the remote server
        Args:
            command (str): packages to run
            stream(bool): Print the output of the packages as
                          a continuous stream

        Returns:
             tuple: (tuple of success, stdout and stderr)

        """
        if not self.ssh_client:
            return False, None, None

        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(command)
            if stream:
                stdin.close()
                for line in iter(lambda: stdout.readline(2048), ""):
                    self.logger.debug(line)
        except paramiko.SSHException as e:
            self.logger.error("Command {} failed to run with exception".format(
                command, e))
            return False, e, e
        return True, stdout, stderr
