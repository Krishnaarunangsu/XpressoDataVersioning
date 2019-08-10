import json

from xpresso.ai.admin.controller.xprobject import XprObject
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.utils.sshutils import SSHUtils
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.cluster_management.xpr_clusters \
    import XprClusters


class Node(XprObject):
    """
    This class represents a User
    """
    package_root_path = "/opt/xpresso.ai"

    def __init__(self, node_json=None):
        self.logger = XprLogger()
        """
        Constructor:
        """
        self.logger.debug(f"Node constructor called with {node_json}")
        super().__init__(node_json)
        print(self.data)
        # These are mandatory fields that needs to be provided in user_json
        self.mandatory_fields = ["address", "name"]
        self.provision_fields = ["address", "nodetype"]
        # fields that should be displayed in the output
        self.display_fields = ["address", "name", "nodetype",
                               "provisionStatus", "activationStatus"]

        self.logger.debug("Node constructed successfully")

    def validate_node_address(self):
        """
        Check if node is accessible.

        Parameters:
            node_json [json]: json with the node info
        """
        # checks if the server address is valid
        # creates a ssh connection
        node = SSHUtils(self.data['address'])
        # checks if the SSHClient has connected to server
        if node.client is None:
            return False
        node.close()
        return True

    def provision_info_check(self):
        nodetypes = ['DEVVM', 'CLUSTER_MASTER', 'CLUSTER_WORKER']
        if 'nodetype' not in self.data or 'address' not in self.data:
            raise IncompleteProvisionInfoException(
                "Please provide both nodetype and address for provision_node"
            )
        elif self.data['nodetype'] not in nodetypes:
            print("invalid node type")
            raise InvalidProvisionInfoException("Invalid node type")
        elif not len(self.data['address']):
            print("Invalid address")
            raise InvalidProvisionInfoException("Invalid node address")

        if self.data['nodetype'] == 'CLUSTER_MASTER':
            if 'cluster' not in self.data:
                raise IncompleteProvisionInfoException(
                    "Cluster info is required for provision of master node"
                )
        elif self.data['nodetype'] == 'CLUSTER_WORKER':
            if 'masterip' not in self.data:
                raise IncompleteProvisionInfoException(
                    "ip address of master node is required"
                    " for provision of worker node"
                )
        elif self.data['nodetype'] == 'DEVVM':
            if 'flavor' not in self.data:
                raise InvalidProvisionInfoException(
                    "Invalid flavor provided for provision of dev environment"
                )
            elif (self.data['flavor'].lower() != 'python') and\
                    (self.data['flavor'].lower() != 'java'):
                raise InvalidProvisionInfoException(
                    "Unknown flavor provided."
                    "Only Python & Java are supported currently"
                )

    def provision_node_check(self, provision_json, persistence_manager):
        node_type = provision_json['nodetype']
        # if node_type != node_info['nodetype']:
        #     return error_codes.invalid_provision_information
        if 'provisionStatus' in self.data and self.data['provisionStatus']:
            raise NodeReProvisionException("Node is already Provisioned")

        if node_type == 'CLUSTER_WORKER':
            masterip = provision_json['masterip']
            master_node = persistence_manager.find(
                'nodes', {"address": masterip}
                )
            if len(master_node) == 0:
                raise InvalidMasterException("Master Node not found")
            elif master_node[0]['nodetype'] != 'CLUSTER_MASTER':
                raise InvalidMasterException(
                    "Provided master info is incorrect"
                    )
            elif not master_node[0]['provisionStatus']:
                raise MasterNotProvisionedException(
                    "Master node not yet provisioned"
                    )

    def provision_node_setup(self):
        """
        Updates the node to a master/worker on kubernetes
        """
        server = self.data['address']
        nodetype = self.data['nodetype']
        node_client = SSHUtils(server)

        if node_client.client is None:
            print("unable to login to server")
            return 0

        if nodetype == 'CLUSTER_MASTER':
            cmd = ("cd {} && git fetch && git reset --hard FETCH_HEAD && "
                   "python3 xpresso/ai/admin/infra/clients/xpr_pkg.py "
                   "--conf config/common.json "
                   "--type install "
                   "--package KubeadmDashboardPackage"
                   .format(self.package_root_path))
            print(cmd)
            std_response = node_client.exec(cmd)
            print(f"\n\n STDERR : \n{std_response['stderr']}\n\n\n")

            provision_status = 1

            if std_response['status'] != 0:
                provision_status = 0
            elif not len(std_response['stdout']) and\
                    len(std_response['stderr']):
                provision_status = 0

            node_client.close()
            return provision_status
        elif nodetype == 'CLUSTER_WORKER':
            masterip = {"master_ip": self.data['masterip']}

            cmd = (
                "cd {} && git fetch && git reset --hard FETCH_HEAD && "
                "python3 xpresso/ai/admin/infra/clients/xpr_pkg.py "
                "--conf config/common.json "
                "--type install --package KubeadmNodePackage --parameters '{}'"
                .format(self.package_root_path, json.dumps(masterip)))

            std_response = node_client.exec(cmd)
            print(f"\n\n STDERR : \n{std_response['stderr']}\n\n\n")
            provision_status = 1
            if std_response['status'] != 0:
                provision_status = 0
            elif not len(std_response['stdout']) and\
                    len(std_response['stderr']):
                provision_status = 0

            node_client.close()
            return provision_status
        else:
            print("entered devvm case")
            if 'flavor' in self.data:
                if self.data['flavor'].lower() == 'python':
                    flavor_package = 'DevelopmentPythonVMPackage'
                elif self.data['flavor'].lower() == 'java':
                    flavor_package = 'DevelopmentJavaVMPackage'
                else:
                    node_client.close()
                    return 0
            else:
                node_client.close()
                return 0

            print("flavor_package is ", flavor_package)

            cmd = ("cd {} && git fetch && git reset --hard FETCH_HEAD && "
                   "python3 xpresso/ai/admin/infra/clients/xpr_pkg.py  "
                   "--conf config/common.json "
                   "--type install "
                   "--package {}"
                   .format(self.package_root_path, flavor_package))

        print("cmd is ", cmd)
        std_response = node_client.exec(cmd)
        print(f"\n\n STDERR : \n{std_response['stderr']}\n\n\n")
        provision_status = 1
        if std_response['status'] != 0:
            provision_status = 0
        elif not len(std_response['stdout']) and len(std_response['stderr']):
            provision_status = 0

        node_client.close()
        return provision_status

    def deprovision_node(self):
        server = self.data['address']
        if "provisionStatus" in self.data and not self.data["provisionStatus"]:
            return 1
        node_client = SSHUtils(server)
        if node_client.client is None:
            print("Unable to login to server.")
            return 0
        node_type = self.data['nodetype']

        if node_type == 'DEVVM':
            flavor = self.data['flavor']
            if flavor.lower() == 'python':
                flavor_package = 'DevelopmentPythonVMPackage'
            elif flavor.lower() == 'java':
                flavor_package = 'DevelopmentJavaVMPackage'
            else:
                node_client.close()
                return 0
            command = (
                "cd {} &&  "
                "python3 xpresso/ai/admin/infra/clients/xpr_pkg.py  "
                "--conf config/common.json "
                "--type uninstall "
                "--package {}".format(self.package_root_path, flavor_package)
            )
        else:
            command = (
                "cd {} && python3 xpresso/ai/admin/infra/clients/xpr_pkg.py "
                "--conf config/common.json "
                "--type uninstall --package KubeadmNodePackage"
                .format(self.package_root_path)
            )

        std_response = node_client.exec(command)
        print(f"\n\n\n {std_response['stderr']} \n\n\n")
        if not len(std_response['stdout']) and len(std_response['stderr']):
            node_client.close()
            return 0

        node_client.close()
        return 1

    def assign_node_to_user(self, user):
        connection = SSHUtils(self.data["address"])
        command = "sudo -S adduser " + user
        password = "abz00ba1nc#123\n"
        new_user_password = "abz00ba1nc\n"
        full_name = "\n"
        room_number = "\n"
        work_phone = "\n"
        home_phone = "\n"
        other = "\n"
        info_correct = "Y"
        stdin, stdout, stderr = connection.exec_client(
            command=command, password=password,
            newuserpassword=new_user_password,
            newuserpassword_confirm=new_user_password,
            Fullname=full_name, RoomNumber=room_number,
            WorkPhone=work_phone, HomePhone=home_phone,
            Other=other, InfoCorrect=info_correct)
        connection.close()
        return stdout, stderr

    @staticmethod
    def update_cluster(provision_info, persistence_manager):
        xpr_cluster = XprClusters(persistence_manager)
        node_type = provision_info['nodetype']
        if node_type == 'CLUSTER_MASTER':
            cluster_info = {
                "name": provision_info["cluster"]
            }
            clusters = xpr_cluster.get_clusters(cluster_info)
            if len(clusters) == 0:
                raise ClusterNotFoundException("This Cluster not found")

            if 'master_nodes' not in clusters[0]:
                master_nodes = {"address": ""}
            elif 'address' not in clusters[0]['master_nodes']:
                master_nodes = {"address": ""}
            else:
                master_nodes = clusters[0]['master_nodes']

            if master_nodes['address'] == provision_info['address']:
                return
            else:
                master_nodes['address'] = provision_info['address']
            # # updates persistence if the master node is not present in clusters
            print("updating cluster info for master_node")
            persistence_manager.update("clusters", cluster_info,
                                            {"master_nodes": master_nodes}
                                            )
        elif node_type == 'CLUSTER_WORKER':
            worker_node = {
                "address": provision_info["address"]
            }
            master_ip = provision_info['masterip']
            master = persistence_manager.find("nodes", {"address": master_ip})
            cluster_name = master[0]["cluster"]
            cluster = xpr_cluster.get_clusters({"name": cluster_name})
            worker_nodes_list = [] if 'worker_nodes' not in cluster[0] else\
                cluster[0]["worker_nodes"]
            for node in worker_nodes_list:
                if node["address"] == worker_node["address"]:
                    return
            # updates persistence if the worker node is not present in clusters already
            worker_nodes_list.append(worker_node)
            persistence_manager.update("clusters", {"name": cluster_name},
                                       {"worker_nodes": worker_nodes_list}
                                      )