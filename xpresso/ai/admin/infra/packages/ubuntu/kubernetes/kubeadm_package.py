""" Package to install Kubeadm on a machine"""

__all__ = ['KubeadmPackage']
__author__ = 'Sahil Malav'

import argparse

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils import linux_utils
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    CommandExecutionFailedException


class KubeadmPackage(AbstractPackage):

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def execute(self):
        """
        installs kubeadm on the machine.
        """
        logger = XprLogger()
        if not linux_utils.check_root():
            logger.fatal("Please run this as root")
        logger.info("Installing Kubeadm...")

        try:
            swapoff = 'swapoff -a'
            self.executor.execute(swapoff)  # turns swap off
            add_key = 'curl -s ' \
                      'https://packages.cloud.google.com/apt/doc/apt-key.gpg ' \
                      '| apt-key add -'
            self.executor.execute(add_key)
            add_list_path = '/etc/apt/sources.list.d/kubernetes.list'
            add_list_content = 'deb https://apt.kubernetes.io/ ' \
                               'kubernetes-xenial main'
            linux_utils.write_to_file(add_list_content, add_list_path, "a")
            install_kubeadm = 'apt-get update && apt-get install ' \
                              '-y kubelet kubeadm kubectl'
            self.executor.execute(install_kubeadm)  # installs kubeadm
            hold_kubeadm = 'apt-mark hold kubelet kubeadm kubectl'
            self.executor.execute(hold_kubeadm)
        except CommandExecutionFailedException as e:
            logger.error("Failed to install Kubeadm. \n{}".format(str(e)))
            return False
        return True

    def status(self, **kwargs):
        self.logger.info("Checking if Kubeadm packages are installed")
        return (self.execute_command(
            "kubeadm version") == 0 and self.execute_command(
            "kubelet --version") == 0 and self.execute_command("kubectl") == 0)

    def cleanup(self):
        reset = 'yes | kubeadm reset'
        self.executor.execute(reset)
        remove = ('sudo apt-get purge -y --allow-change-held-packages'
                  ' kubelet kubeadm kubectl')
        self.executor.execute(remove)

    def install(self, **kwargs):
        self.execute()

    def uninstall(self, **kwargs):
        self.cleanup()

    def start(self, **kwargs):
        self.execute()

    def stop(self, **kwargs):
        self.cleanup()


def parse_arguments():
    """ Reads commandline argument to identify which clients group to
    install
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--run",
                        required=False,
                        help="Method to execute")

    return parser


if __name__ == "__main__":
    kubeadm = KubeadmPackage()

    parser = parse_arguments()
    args = parser.parse_args()
    if 'run' in args:
        if args.run == 'execute':
            kubeadm.execute()
        elif args.run == 'clean':
            kubeadm.cleanup()
        else:
            kubeadm.execute()
    else:
        kubeadm.execute()
