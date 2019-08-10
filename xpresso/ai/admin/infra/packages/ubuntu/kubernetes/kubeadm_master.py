""" Package to initialise the master node on a kubernetes cluster"""

__all__ = ['KubeadmMasterPackage']
__author__ = 'Sahil Malav'

import os
import argparse
import time

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.controller.persistence.mongopersistencemanager import \
    MongoPersistenceManager
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils import linux_utils
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import\
    CommandExecutionFailedException


class KubeadmMasterPackage(AbstractPackage):
    KUBE_SECTION = 'kubernetes'
    CIDR_KEY = 'pod_network_cidr'
    CONTROLLER_SECTION = 'controller'
    MONGO_SECTION = 'mongodb'
    PROJECTS_SECTION = 'projects'
    URL = 'mongo_url'
    DB = 'database'
    UID = 'mongo_uid'
    PWD = 'mongo_pwd'
    PACKAGES = 'packages_setup'
    W = 'w'

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)
        self.persistence_manager = MongoPersistenceManager(
                              url=self.config[self.MONGO_SECTION][self.URL],
                              db=self.config[self.MONGO_SECTION][self.DB],
                              uid=self.config[self.MONGO_SECTION][self.UID],
                              pwd=self.config[self.MONGO_SECTION][self.PWD],
                              w=self.config[self.MONGO_SECTION][self.W])

    def execute(self):
        """
        installs kubernetes master node on the machine.
        """

        logger = XprLogger()
        if not linux_utils.check_root():
            logger.fatal("Please run this as root")
        logger.info("Initialising Kubernetes master node...")
        try:
            pod_network_cidr = self.config[self.PACKAGES][self.KUBE_SECTION][self.CIDR_KEY]
            init = 'kubeadm init --token-ttl=0 --pod-network-cidr={}'.format(
                pod_network_cidr)
            (_, output, _) = self.executor.execute_with_output(init)
            output = output.splitlines()
            join_command = (output[-2].decode("utf-8").rstrip('\\') +
                            output[-1].decode("utf-8"))
            # waiting time for master node to become active
            time.sleep(90)
            master_ip = linux_utils.get_ip_address()
            cluster_path = '/mnt/nfs/data/k8/k8_clusters/' \
                           '{}'.format(master_ip)
            linux_utils.create_directory(cluster_path, 0o755)
            join_filename = '{}/{}.txt'.format(cluster_path, master_ip)
            linux_utils.write_to_file(join_command, join_filename, "w+")
            if not os.path.isfile(join_filename):
                logger.error('Failed to write join command to file. Exiting.')
                raise CommandExecutionFailedException
            kubeconfig = 'KUBECONFIG=/etc/kubernetes/admin.conf'
            environment_path = '/etc/environment'
            linux_utils.write_to_file(kubeconfig, environment_path, "a")
            os.environ["KUBECONFIG"] = "/etc/kubernetes/admin.conf"
            kube_directory = '$HOME/.kube'
            linux_utils.create_directory(kube_directory, 0o755)
            copy_config = 'sudo cp -f /etc/kubernetes/admin.conf' \
                          ' $HOME/.kube/config'
            self.executor.execute(copy_config)
            chown = 'sudo chown $(id -u):$(id -g) $HOME/.kube/config'
            self.executor.execute(chown)
            flannel = 'kubectl apply -f https://raw.githubusercontent.com' \
                      '/coreos/flannel/master/Documentation/kube-flannel.yml'
            self.executor.execute(flannel)
            generate_api_token = "kubectl get secret $(kubectl get " \
                                 "serviceaccount default -o jsonpath=" \
                                 "'{.secrets[0].name}') -o jsonpath=" \
                                 "'{.data.token}' | base64 --decode"
            status, stdout, stderr = self.executor.execute_with_output(
                generate_api_token)
            if status != 0 or len(stderr.decode('utf-8')):
                raise CommandExecutionFailedException("Token generation failed")
            token = stdout.decode("utf-8")
            self.persistence_manager.update("nodes", {"address": master_ip},
                                 {"token": token})
            api_access = 'kubectl create clusterrolebinding permissive-binding \
                                  --clusterrole=cluster-admin \
                                  --user=admin \
                                  --user=kubelet \
                                  --group=system:serviceaccounts'
            self.executor.execute(api_access)
            docker_secret = \
                'kubectl create secret docker-registry dockerkey ' \
                '--docker-server https://dockerregistry.xpresso.ai/ ' \
                '--docker-username xprdocker --docker-password Abz00ba@123'
            self.executor.execute(docker_secret)

        except CommandExecutionFailedException as e:
            logger.error("Failed to initialise master. \n{}".format(str(e)))
            return False
        return True

    def status(self, **kwargs):
        self.logger.info("Checking if kubernes master is installe "
                         "packages are installed")
        return self.execute_command("kubectl cluster-info") == 0

    def cleanup(self):
        reset = 'yes | kubeadm reset'
        self.executor.execute(reset)
        restart = 'systemctl restart kubelet'
        self.executor.execute(restart)

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
    kube_master = KubeadmMasterPackage()

    parser = parse_arguments()
    args = parser.parse_args()
    if 'run' in args:
        if args.run == 'execute':
            kube_master.execute()
        elif args.run == 'clean':
            kube_master.cleanup()
        else:
            kube_master.execute()
    else:
        kube_master.execute()