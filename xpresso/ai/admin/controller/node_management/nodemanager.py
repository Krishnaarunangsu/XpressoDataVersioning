from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.utils.xprresponse import XprResponse
from xpresso.ai.admin.controller.utils import error_codes
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.node_management.node import Node
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.user_management.usermanager import UserManager
from xpresso.ai.admin.controller.utils.sshutils import SSHUtils


class NodeManager:
    config_path = XprConfigParser.DEFAULT_CONFIG_PATH

    def __init__(self, persistence_manager):
        self.config = XprConfigParser(self.config_path)
        self.logger = XprLogger()
        self.persistence_manager = persistence_manager

    def register_node(self, node_json):
        """
            registers a new node in the database if the server is available

            checks if the node already exists and then checks if the server
            with node ip_address is available. Then adds the node to database

            Parameters:
                node_json [json]: json with node information

            Return:
                Success -> 'OK' [str] : returns 'OK' as response
                Failure -> [str] : returns appropriate failure response
        """
        self.logger.info("registering a new node")
        self.logger.debug(f"node info provided is : {node_json}")
        new_node = Node(node_json)
        self.logger.info('checking if the mandatory fields are provided')
        new_node.validate_mandatory_fields()
        self.logger.info('checking if the address of node is valid')
        new_node.validate_node_address()
        new_node.set('provisionStatus', False)
        new_node.set('activationStatus', True)

        self.logger.info("provisionStatus and activationStatus fields are set")

        self.logger.info("adding node to the database")

        self.persistence_manager.insert("nodes", new_node.data, False)
        self.logger.info("node successfully added to the persistence")

    def get_nodes(self, filter_json, apply_display_filter=True):
        """
            Gets the list of nodes in the database

            Calls the persistence with input filters to fetch the list of nodes
            After fetching, the nodes list is filtered before sending
            as output in order to send relevant information only

            Parameters:
                filter_json [json] : json with filter key & value pairs

            Return:
                Success -> [list] : returns list of nodes
                Failure -> [str] : returns persistence failure response
        """
        self.logger.debug(f"filter_json is : {filter_json}")
        self.logger.info("getting the list of nodes from persistence")
        nodes = self.persistence_manager.find("nodes", filter_json)
        self.logger.info("filtering nodes before sending as output")
        if apply_display_filter:
            filtered_nodes = []
            for node_json in nodes:
                temp_node = Node(node_json)
                temp_node.filter_display_fields()
                filtered_nodes.append(temp_node.data)
            nodes = filtered_nodes
        self.logger.debug("Output of Nodes sent : ", nodes)
        return nodes

    def provision_node(self, provision_json):
        """
            provisions a node either for deployment or development

            checks if node is available and then connects to the server
            through ssh and runs appropriate packages on the server

            Parameters:
                node_id [str]: id i.e. name of the node
                provision_json [json] : json with node provision info

            Return:
                Success -> 'OK' [str] : returns OK if provision_node succeeds
                Failure -> [str] : returns appropriate failure response
        """
        self.logger.debug(f"provision_node info is: {provision_json}")
        self.logger.info("provision of a node is requested")
        new_node = Node(provision_json)
        new_node.provision_info_check()
        address = provision_json["address"]
        node_id_json = {"address": address}
        node_type = provision_json['nodetype']
        self.logger.info("checking persistence if the node is registered")
        node = self.persistence_manager.find("nodes", node_id_json)
        if len(node) == 0:
            self.logger.error("Node not found")
            raise NodeNotFoundException("Node not found to provision")

        for (key, val) in node[0].items():
            new_node.set(key, val)

        print(new_node.data)
        new_node.provision_node_check(provision_json, self.persistence_manager)
        print("provision_node_check passed")
        # checks if ip address of the node and its type is provided or not
        if node_type != 'DEVVM':
            print("updating cluster")
            new_node.update_cluster(provision_json, self.persistence_manager)
        self.logger.info("provision node in progress")
        provision_status = new_node.provision_node_setup()
        if provision_status == 1:
            self.logger.info("provision of node is successful")
            update_json = {
                "provisionStatus": True,
                "nodetype": node_type
            }
            if node_type == 'CLUSTER_MASTER':
                update_json["cluster"] = provision_json["cluster"]
            elif node_type == 'CLUSTER_WORKER':
                update_json["masterip"] = provision_json["masterip"]
            else:
                update_json["flavor"] = provision_json["flavor"].lower()

            self.persistence_manager.update("nodes", node_id_json, update_json)
        elif provision_status == 0:
            self.logger.error("provision failed: kubernetes error")
            raise ProvisionKubernetesException("Provision Failed")
        else:
            self.logger.error('given provision node data is invalid')
            raise InvalidProvisionInfoException("Provision data is invalid")

    def deactivate_node(self, node_id):
        """
            Deactivates a node in persistence

            Deletes all the installed packages of the node on server
            then deactivates the node in database

            Parameters:
                node_id [str] : name of the node

            Return:
                returns appropriate output
        """
        self.logger.info(f"request received for deactivating node {node_id}")
        node_id_json = {
            "address": node_id
        }
        self.logger.info("checking persistence if node is present or not")
        nodes = self.persistence_manager.find("nodes", node_id_json)

        if not len(nodes):
            raise NodeNotFoundException("Node not found for deactivation")

        if 'activationStatus' in nodes[0] and not nodes[0]['activationStatus']:
            self.logger.error("This node is already deactivated")
            raise NodeDeactivatedException()

        new_node = Node(nodes[0])

        # deletes all the packages installed on the node
        self.logger.info("deleting all packages on the node")
        node_deprovision = 1
        if new_node.data["provisionStatus"]:
            # deprovision shall be called only on provisioned nodes
            node_deprovision = new_node.deprovision_node()
        if node_deprovision == 1:
            self.logger.info("deleted all of the packages on node")
            # deletes the node entry from the database
            self.logger.info('deactivating node from persistence')
            deactivate_json = {
                "activationStatus": False,
                "provisionStatus": False
            }
            self.persistence_manager.update(
                "nodes", node_id_json, deactivate_json
            )
            return XprResponse('success', '', {})
        else:
            self.logger.error('Node deletion failed: kubernetes error')
            raise ProvisionKubernetesException("Deactivation Failed")

    def assign_node(self, assign_json):
        """
            assigns a node to a user

            assigns a node with development vm type to a user

            Parameters:
                assign_json [json] : Json with assignation info

            Return:
                returns appropriate output
        """
        if 'user' not in assign_json or 'node' not in assign_json:
            self.logger.error("Incomplete information in assign_json")
            raise IncompleteNodeInfoException("user and node info is required")
        elif not len(assign_json['user']) or not len(assign_json['node']):
            self.logger.error("Incomplete information in assign_json")
            raise IncompleteNodeInfoException(
                "user & node info shouldn't be empty"
            )

        uid_json = {"address": assign_json['node']}
        user = assign_json['user']
        users = UserManager(self.persistence_manager).get_users({"uid": user})
        nodes = self.persistence_manager.find('nodes', uid_json)
        if len(users) == 0:
            raise UserNotFoundException("User not found")
        elif len(nodes) == 0:
            raise NodeNotFoundException("Node not found")
        else:
            if 'provisionStatus' not in nodes[0]:
                raise UnProvisionedNodeException("Node is not provisioned")
            elif not nodes[0]['provisionStatus']:
                raise UnProvisionedNodeException("Node is not provisioned")
            elif nodes[0]['nodetype'] != 'DEVVM':
                raise NodeTypeException(
                    "Assign only work form node types of devvm")

            user_nodes = []
            for node_dict in users[0]['nodes']:
                user_nodes.append(node_dict['address'])
            if assign_json['node'] in user_nodes:
                raise NodeAlreadyAssignedException()

        new_node = Node(nodes[0])
        out, err = new_node.assign_node_to_user(user)
        try:
            if not len(out.readlines()) and len(err.readlines()):
                print("failure because of errors")
                raise NodeAssignException(
                    "Assignation failed due to internal error"
                )
            else:
                temp_node = {
                    'address': nodes[0]['address']
                }
                nodes = [] if 'nodes' not in users[0] else users[0]['nodes']
                nodes.append(temp_node)
                self.persistence_manager.update('users', {"uid": user},
                                                {"nodes": nodes})
        except:
            print("caught exception")
            raise NodeAssignException(
                "Assignation failed due to internal error"
            )

    def modify_node(self, changes_json):
        """
            modify_node updates the node info in the persistence

            checks if node is available and then updates
            the info as per changes_json

            Parameters:
                changes_json [json] : json with node changes info

            Return:
                returns xprresponse object
        """
        if 'address' not in changes_json:
            raise IncompleteNodeInfoException("Node address not provided")

        uid_json = {"address": changes_json['address']}
        self.logger.info(f"Modifying node information of {uid_json}")
        self.logger.debug(f"Info provided to be modified is {changes_json}")
        # checks if the user is present in persistence
        self.logger.info("Checking if the node is present in the persistence")
        node = self.persistence_manager.find("nodes", uid_json)
        if len(node) == 0:
            self.logger.error(
                f"node {uid_json['address']} not found in the persistence")
            raise NodeNotFoundException()

        if 'activationStatus' in changes_json and \
            not changes_json['activationStatus']:
            raise CallDeactivateNodeException()

        self.logger.info("updating the user information")
        self.persistence_manager.update("nodes", uid_json, changes_json)

    def delete_node(self, node_id):
        """
            deletes the node from persistence

            Deletes all the installed packages of the node on server
            then deletes the node from database

            Parameters:
                node_id [str] : name of the node

            Return:
                returns appropriate output
        """
        self.logger.info(f"request received for deactivating node {node_id}")
        node_id_json = {
            "address": node_id
        }
        self.logger.info("checking persistence if node is present or not")
        nodes = self.persistence_manager.find("nodes", node_id_json)

        if nodes and len(nodes):
            self.logger.info("deleting all packages on the node")
            new_node = Node(nodes[0])
            node_deletion = new_node.deprovision_node()
            if node_deletion == 1:
                self.logger.info("deleted all of the packages on node")
                # deletes the node entry from the database
                self.logger.info('deleting node from persistence')
                self.persistence_manager.delete("nodes", node_id_json)
            else:
                self.logger.error('Node deletion failed: kubernetes error')
                raise NodeDeletionKubernetesException()
        else:
            raise NodeNotFoundException()

    def update_all_nodes(self, filter_json=None, branch_name="master"):
        """
        Update the xpresso project in all the nodes
        Args:
            filter_json: dictionary to updated specific set of nodes
            branch_name: name of the branch to which xpresso project will be
                         updated

        Returns:
            (list, list): list of update node and list of non updated node
        """

        if filter_json is None:
            filter_json = {}
        filtered_node_list = self.get_nodes(filter_json=filter_json)
        updated_list = []
        non_updated_list = []

        update_cmd = (f"cd {self.config['general']['package_path']} && "
                      f"python3 xpresso/ai/admin/infra/clients/xpr_pkg.py "
                      f"--conf config/common.json "
                      f"--type install "
                      f"--package UpdateLocalXpressoPackage "
                      f"--parameters {{\"branch_name\": \"{branch_name}\"}}' && "
                      f"cp config/common_{self.config['env']}.json "
                      f"config/common.json ")
        self.logger.debug(update_cmd)
        for node in filtered_node_list:
            node_address = node["address"]
            ssh_client = SSHUtils(node_address)

            if ssh_client.client is None:
                self.logger.warning(
                    f"unable to login to server: {node_address}")
                non_updated_list.append(node_address)
                continue
            std_response = ssh_client.exec(update_cmd)
            self.logger.debug(f"\n\n STDERR : \n{std_response['stderr']}\n")
            if std_response['status'] == 0:
                updated_list.append(node_address)
            else:
                non_updated_list.append(node_address)
            ssh_client.close()
        return updated_list, non_updated_list
