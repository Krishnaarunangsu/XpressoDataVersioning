""" Package to join a worker node to an existing kubernetes cluster"""

__all__ = ['KubeadmNodePackage']
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


class KubeadmNodePackage(AbstractPackage):
    PARAMETER_MASTER_IP = "master_ip"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def execute(self, parameters=None):
        """
        installs worker node on the machine.
        """
        logger = XprLogger()
        if not linux_utils.check_root():
            logger.fatal("Please run this as root")

        self.cleanup()
        logger.info("Initialising Kubernetes worker node...")
        try:

            if parameters and self.PARAMETER_MASTER_IP in parameters:
                master_ip = parameters[self.PARAMETER_MASTER_IP]
            else:
                master_ip = input("Enter the IP address of the master"
                                  " node you want to join:")
            path = '/mnt/nfs/data/k8/k8_clusters/{}/{}.txt'. \
                format(master_ip, master_ip)
            with open(path, "r") as f:
                join_command = f.read()  # extract join command
            self.executor.execute(
                join_command)  # run command to join the cluster
        except CommandExecutionFailedException as e:
            logger.error("Failed to setup worker node. \n{}".format(str(e)))
            return False
        return True

    def status(self, **kwargs):
        return False

    def cleanup(self):
        reset = 'yes | kubeadm reset'
        self.executor.execute(reset)
        kube_restart = ('systemctl stop kubelet; systemctl stop docker;'
                        'rm -rf /var/lib/cni/; rm -rf /var/lib/kubelet/*; '
                        'rm -rf /etc/cni/; '
                        'ifconfig cni0 down; ifconfig flannel.1 down;'
                        'ifconfig docker0 down'
                        'systemctl start kubelet; systemctl start docker;'
                        'ip link delete cni0;ip link delete flannel.1;')

        self.executor.execute(kube_restart)

    def install(self, parameters=None):
        self.execute(parameters=parameters)

    def uninstall(self, parameters=None):
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
    parser.add_argument("--masterip",
                        required=False,
                        help="Ip address of the master node to join to")

    return parser


if __name__ == "__main__":
    kube_node = KubeadmNodePackage()

    parser = parse_arguments()
    args = parser.parse_args()
    masterip = None if not args.masterip else args.masterip
    if 'run' in args:
        if args.run == 'execute':
            kube_node.execute(masterip)
        elif args.run == 'clean':
            kube_node.cleanup()
        else:
            kube_node.execute(masterip)
    else:
        kube_node.execute(masterip)
