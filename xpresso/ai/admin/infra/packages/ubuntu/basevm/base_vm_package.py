"""Abstract base class for packages object"""
import subprocess
from time import sleep

__all__ = ['BaseVMPackage']
__author__ = 'Naveen Sinha'

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.core.utils.linux_utils import check_root
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser


class BaseVMPackage(AbstractPackage):
    """
    Placeholder package to install dependent packages
    """

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)

    def status(self, **kwargs):
        return True

    def install(self, **kwargs):
        """
        - Sets up the root password
        - Installs xprctl
        """

        if not check_root():
            raise PermissionError("Need sudo permission")

        # Setup the root password
        root_user = self.config["vms"]["username"]
        root_pass = self.config["vms"]["password"]
        try:
            self.set_password(root_user, root_pass)
        except RuntimeError:
            raise RuntimeError("Password Setup Failed")

        # Updating sshd service to allow remote root access
        with open("/etc/ssh/sshd_config", "a") as ssh_conf:
            ssh_conf.write("\nPermitRootLogin yes\n")
        self.execute_command("service sshd restart")
        # Assumption, it is being run from the root folder
        self.execute_command("pip3 install .")

        return True

    def uninstall(self, **kwargs):
        return True

    def start(self, **kwargs):
        return True

    def set_password(self, user, password):
        cmd = ["/usr/bin/passwd", user]
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        print(password)
        p.stdin.write(str.encode(f'{password}\n{password}\n'))
        p.stdin.flush()
        # Give `passwd` cmd 5 second to finish and kill it otherwise.
        for x in range(0, 5):
            if p.poll() is not None:
                break
            sleep(1)
        else:
            p.terminate()
            sleep(1)
            p.kill()
            raise RuntimeError('Setting password failed. '
                               '`passwd` process did not terminate.')
        if p.returncode != 0:
            raise RuntimeError('`passwd` failed: %d' % p.returncode)

    def stop(self, **kwargs):
        return True
