# paramiko package is required for connecting to a server through ssh

from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
import paramiko
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import\
    InvalidNodeException, BadHostkeyException, UnexpectedNodeException

config_path = XprConfigParser.DEFAULT_CONFIG_PATH
config = XprConfigParser(config_path)


class SSHUtils:
    def __init__(self, server):
        self.server = server
        self.client = self.connect(server)

    def connect(self, server):
        """
            creates a connection to the server through ssh

            Parameters:
                server [str]: ip_address of the server to connect to

            Return:
                [SSHClient object]: returns SSHClient instance
        """
        try:
            # SSHClient class represents a session with ssh server
            client = paramiko.SSHClient()
            # In case host key is missing in known_hosts
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                server,
                username=config['vms']['username'],
                password=config['vms']['password']
            )
            return client
        except paramiko.AuthenticationException as exc:
            print(f"Server Authentication failed : \n {exc}")
            print(f"username : {config['vms']['username']}")
            print(f"password : {config['vms']['password']}")
            raise InvalidNodeException
        except (paramiko.BadHostKeyException, paramiko.SSHException) as exc:
            # checks if the connection to the server succeeds
            # if connection is made then host key dict is updated
            print("Server connection failed : \n ", exc)
            raise BadHostkeyException
        except Exception as e:
            print("\n \n Error is : ", e)
            raise UnexpectedNodeException

    def exec(self, command):
        """
            executes a command on server through channel of SSHClient

            creates a new channel over the SSHClient connection
            then executes the command over this channel

            Parameters:
                command [str] : command to be executed on server

            Return:
                returns stdin, stdout, stderr of command execution process
        """
        # creates a new tunnel over connection to ssh server
        channel = self.client.get_transport().open_session()

        # channel.set_environment_variable(
        #     'PYTHONPATH',
        #     '/home/xprops/xpresso.ai'
        # )
        # executes the command over the channel

        channel.exec_command(command)
        # waits till the process executing the command is complete
        stdout = ''
        while not channel.exit_status_ready():
            tempout = channel.recv(4294967296).decode('utf-8')
            print(tempout)
            stdout = stdout + tempout

        stderr = channel.recv_stderr(4294967296).decode('utf-8')
        status = channel.recv_exit_status()
        print("status is ", status)
        return {'status': status, 'stdout': stdout, 'stderr': stderr}

    def exec_client(self, **kwargs):

        stdin_values = list()
        for key, value in kwargs.items():
            if key == "command":
                command = value
            else:
                stdin_values.append(value)
        try:
            stdin, stdout, stderr = self.client.exec_command(command)

            for val in stdin_values:
                stdin.write(val)

            print("exiting exec_client")
            return (stdin, stdout, stderr)
        except:
            return (None, None, None)

    def close(self):
        self.client.close()
