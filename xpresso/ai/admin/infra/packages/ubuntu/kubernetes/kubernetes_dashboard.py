""" Package to deploy the dashboard on a kubernetes cluster"""

__all__ = ['KubeadmDashboardPackage']
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


class KubeadmDashboardPackage(AbstractPackage):

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def execute(self):
        """
        installs kubernetes dashboard on the machine.
        """
        logger = XprLogger()
        if not linux_utils.check_root():
            logger.fatal("Please run this as root")
        logger.info("Setting up the Kubernetes dashboard...")
        try:
            deploy_dashboard = 'kubectl create -f https://raw.githubusercontent' \
                               '.com/kubernetes/dashboard/master/aio/deploy' \
                               '/recommended/kubernetes-dashboard.yaml'
            self.executor.execute(deploy_dashboard)  # creates deployment
            nodeport = """kubectl -n kube-system patch service \
                    kubernetes-dashboard --type='json' -p \
                    '[{"op":"replace","path":"/spec/type","value":"NodePort"}]'"""
            self.executor.execute(nodeport)  # exposes dashboard
            constant_port = """kubectl -n kube-system patch service \
                    kubernetes-dashboard --type='json' -p \
                    '[{"op":"replace","path":"/spec/ports/0/nodePort","value":30252}]'"""
            self.executor.execute(constant_port)  # sets constant port
            content_path = '/opt/xpresso.ai/config/kubernetes-dashboard-access.yaml'
            with open(content_path, "r") as f:
                content = f.read()
            path = '/etc/kubernetes/kube-dashboard-access.yaml'
            linux_utils.write_to_file(content, path, "w+")
            dashboard_access = 'kubectl create -f {}'.format(path)
            self.executor.execute(dashboard_access)  # grants permission
            skip_login = """kubectl patch deployment -n kube-system \
                    kubernetes-dashboard --type='json' -p='[{"op": "add", "path": \
                    "/spec/template/spec/containers/0/args/1", \
                    "value":"--enable-skip-login" }]'"""
            self.executor.execute(skip_login)  # enables skip login
        except CommandExecutionFailedException as e:
            logger.error("Failed to setup dashboard. \n{}".format(str(e)))
            return False
        return True

    def status(self, **kwargs):
        pass

    def cleanup(self):
        deployment = 'kubectl delete deployment ' \
                     'kubernetes-dashboard --namespace=kube-system'
        self.executor.execute(deployment)
        service = 'kubectl delete service ' \
                  'kubernetes-dashboard  --namespace=kube-system'
        self.executor.execute(service)
        role = 'kubectl delete role ' \
               'kubernetes-dashboard-minimal --namespace=kube-system'
        self.executor.execute(role)
        rolebinding = 'kubectl delete rolebinding ' \
                      'kubernetes-dashboard-minimal --namespace=kube-system'
        self.executor.execute(rolebinding)
        sa = 'kubectl delete sa ' \
             'kubernetes-dashboard --namespace=kube-system'
        self.executor.execute(sa)
        secret = 'kubectl delete secret ' \
                 'kubernetes-dashboard-certs --namespace=kube-system'
        self.executor.execute(secret)
        secret_key = 'kubectl delete secret ' \
                     'kubernetes-dashboard-key-holder --namespace=kube-system'
        self.executor.execute(secret_key)

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
    kube_dashboard = KubeadmDashboardPackage()

    parser = parse_arguments()
    args = parser.parse_args()
    if 'run' in args:
        if args.run == 'execute':
            kube_dashboard.execute()
        elif args.run == 'clean':
            kube_dashboard.cleanup()
        else:
            kube_dashboard.execute()
    else:
        kube_dashboard.execute()
