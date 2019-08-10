from xpresso.ai.admin.controller.utils import error_codes


class XprExceptions(BaseException):
    """ General Xpresso exception occurred """

    def __init__(self, message: str = None):
        self.message = message
        self.error_code = 777

    def __str__(self):
        """
        gets a string representation of this exception
        :return: string representation of exception
        """
        return self.message


class UserNotFoundException(XprExceptions):
    """
    class for exception thrown when the requested user is not found in the
    persistence.
    """

    def __init__(self, message: str = "User not found"):
        self.message = message
        self.error_code = error_codes.user_not_found


class DeactivatedUserException(XprExceptions):
    """
    class for exception thrown when the requested user is not found in the
    persistence.
    """

    def __init__(self, message: str = "User already deactivated"):
        self.message = message
        self.error_code = error_codes.user_already_deactivated


class LogoutFailedException(XprExceptions):
    """
    class for exception thrown when logout request fails.
    """


class WrongPasswordException(XprExceptions):
    """
    class for exception thrown when the authentication fails
    """

    def __init__(self, message: str = "Wrong Password"):
        self.message = message
        self.error_code = error_codes.wrong_pwd


class AuthenticationFailedException(XprExceptions):
    """
    class for exception thrown when the authentication fails
    """

    def __init__(self, message: str = "Authentication Failed"):
        self.message = message
        self.error_code = error_codes.auth_failed


class AlreadyLoggedInException(XprExceptions):
    def __init__(self, message: str = "Already Logged In"):
        self.message = message
        self.error_code = error_codes.already_logged_in


class UnsuccessfulConnectionException(XprExceptions):
    """
    Class to define an exception thrown when a database connection was
    unsuccessful
    """

    def __init__(self, message: str = "Database Connection Issue"):
        self.message = message
        self.error_code = error_codes.unsuccessful_connection


class UnsuccessfulOperationException(XprExceptions):
    """
    Class to define an exception thrown when a database operation was
    unsuccessful. Usually indicates a duplicate key error.
    """

    def __init__(self, message: str = "Unsuccessful Operation"):
        self.message = message
        self.error_code = error_codes.unsuccessful_operation


class ClusterNameBlankException(XprExceptions):
    """
    Thrown when user provides blank cluster name while registering
    """

    def __init__(self, message: str = "Cluster name is invalid"):
        self.message = message
        self.error_code = error_codes.cluster_name_blank


class ClusterAlreadyExistsException(XprExceptions):
    """
    class for exception thrown when the cluster to be registered already exists
    """

    def __init__(self, message: str = "Cluster already exists"):
        self.message = message
        self.error_code = error_codes.cluster_already_exists


class ClusterNotFoundException(XprExceptions):
    """
    class for exception thrown when the cluster to be deleted doesn't exist
    """

    def __init__(self, message: str = "Cluster not found"):
        self.message = message
        self.error_code = error_codes.cluster_not_found


class IncompleteClusterInfoException(XprExceptions):
    """
    class for exception thrown when the cluster information isn't sufficient
    """

    def __init__(self, message: str = "Cluster info incomplete"):
        self.message = message
        self.error_code = error_codes.incomplete_cluster_info


class BuildRequestFailedException(XprExceptions):
    """
    class for exception thrown when project build fails.
    """

    def __init__(self, message: str = "Project build failed"):
        self.message = message
        self.error_code = error_codes.project_build_failed


class DeployRequestFailedException(XprExceptions):
    """
    class for exception thrown when project deployment fails.
    """

    def __init__(self, message: str = "Deploy Request Failed"):
        self.message = message
        self.error_code = error_codes.project_deployment_failed


class BlankFieldException(XprExceptions):
    """
    Class to define an exception thrown when a field is 
    blank in an object input
    """

    def __init__(self, message: str = "Blank Field Provided"):
        self.message = message
        self.error_code = error_codes.blank_field_error


class MissingFieldException(XprExceptions):
    """
    Class to define an exception thrown when a field is missing from an object
    input
    """

    def __init__(self, message: str = "One or more field is missing"):
        self.message = message
        self.error_code = error_codes.missing_field_error


class ExpiredTokenException(XprExceptions):
    """
    Class to define an exception thrown when the authentication token supplied
    has expired
    """

    def __init__(self, message: str = "Token is expired. Relogin"):
        self.message = message
        self.error_code = error_codes.expired_token


class IncorrectTokenException(XprExceptions):
    """
    Class to define an exception thrown when the authentication token supplied
    is wrong
    """

    def __init__(self, message: str = "Token is invalid. Relogin"):
        self.message = message
        self.error_code = error_codes.wrong_token


class TokenNotSpecifiedException(XprExceptions):
    """
    Class to define an exception thrown when an authentication token
    is not specified
    """

    def __init__(self, message: str = "Token not provided"):
        self.message = message
        self.error_code = error_codes.wrong_token


class PermissionDeniedException(XprExceptions):
    """
    Class to define an exception thrown when the user is denied permission for
    the specified action
    """

    def __init__(self, message: str = "Permission Denied"):
        self.message = message
        self.error_code = error_codes.permission_denied


class InvalidValueException(XprExceptions):
    """
    Class to define an exception thrown when a the value of a field is invalid
    in an object input
    """

    def __init__(self, message: str = "Invalid value provided"):
        self.message = message
        self.error_code = error_codes.invalid_value_error


class InvalidUserIDException(XprExceptions):
    """
    Class to define an exception thrown when the user id is either not
    specified or blank
    """

    def __init__(self, message: str = "Invalid  user id"):
        self.message = message
        self.error_code = error_codes.empty_uid


class InvalidNodeException(XprExceptions):
    def __init__(self, message: str = "Invalid  user node"):
        self.message = message
        self.error_code = error_codes.invalid_node_data


class UnexpectedNodeException(XprExceptions):
    """
        This exception is called when the host key for the server address
        provided is unavailable
    """

    def __init__(self, message: str = "Node unavailable"):
        self.message = message
        self.error_code = error_codes.node_not_found


class BranchNotSpecifiedException(XprExceptions):
    """
    Thrown when no branch is specified while building a project
    """

    def __init__(self, message: str = "Branch not specified"):
        self.message = message
        self.error_code = error_codes.branch_not_specified


class InvalidBuildVersionException(XprExceptions):
    """
    Thrown when build version is not specified while deploying a project
    """

    def __init__(self, message: str = "Invalid build version"):
        self.message = message
        self.error_code = error_codes.invalid_build_version


class IncompleteProjectInfoException(XprExceptions):
    """
    Thrown when user has provided insufficient info while building/deploying
    """

    def __init__(self, message: str = "Incomplete project info"):
        self.message = message
        self.error_code = error_codes.incomplete_project_information


class ComponentsSpecifiedIncorrectlyException(XprExceptions):
    """
    Thrown when components to be built/deployed are specified incorrectly
    """

    def __init__(self, message: str = "Component specified incorrectly"):
        self.message = message
        self.error_code = error_codes.components_specified_incorrectly


class CurrentlyNotDeployedException(XprExceptions):
    """
    Thrown when user attempts to undeploy a project which isn't currently
    deployed
    """

    def __init__(self, message: str = "Currently not deployed"):
        self.message = message
        self.error_code = error_codes.currently_not_deployed


class ClusterRequestFailedException(XprExceptions):
    """
    class for exception thrown when the requested cluster is
    not found in the db.
    """


class HTTPRequestFailedException(XprExceptions):
    """
    class for exception thrown when HTTP request fails
    """


class HTTPInvalidRequestException(XprExceptions):
    """
    class for exception thrown when HTTP request is invalid
    """


class CLICommandFailedException(XprExceptions):
    """
    throws when the cli command fails
    """


class ControllerClientResponseException(XprExceptions):
    """
    throws when the controller client fails
    """

    def __init__(self, message: str, error_code: int):
        self.message = message
        self.error_code = error_code

    def __str__(self):
        """
        gets a string representation of this exception
        return:
            string representation of exception
        """
        return f"Code {self.error_code}: {self.message}"


class ProjectFormatException(XprExceptions):
    """
    This exception is called when the project format is not valid
    """

    def __init__(self, message: str = "Project format is invalid"):
        self.message = message
        self.error_code = error_codes.invalid_project_format


class ProjectConfigException(XprExceptions):
    """
    This exception is called when the project config file is not
    loaded correctly
    """

    def __init__(self, message: str = "Project configuration is invalid"):
        self.message = message
        self.error_code = error_codes.internal_config_error


class ProjectFieldsException(XprExceptions):
    """
    This exception is called when the input fields provided are not
    following the format
    """

    def __init__(self, message: str = "Invalid project field format"):
        self.message = message
        self.error_code = error_codes.invalid_project_field_format


class DuplicateComponentException(XprExceptions):
    """
    This exception is called when there is request for a duplicate component
    """

    def __init__(self, message: str = "Component alread exist"):
        self.message = message
        self.error_code = error_codes.component_already_exists


class ComponentFieldException(XprExceptions):
    """
    This exception is called when there are bugs with component field's
    format
    """

    def __init__(self, message: str = "Unkown component specified"):
        self.message = message
        self.error_code = error_codes.unknown_component_key


class ProjectOwnerException(XprExceptions):
    """
    This exception is called if the owner information is incorrect
    """

    def __init__(self, message: str = "Invalid project owner"):
        self.message = message
        self.error_code = error_codes.invalid_owner_information


class BadHostkeyException(XprExceptions):
    """
    This exception is called when the host key for the server address
    provided is unavailable
    """

    def __init__(self, message: str = "Node not found"):
        self.message = message
        self.error_code = error_codes.node_not_found


class ProjectNotFoundException(XprExceptions):
    """
    Thrown when project to be deployed/built is not found
    """

    def __init__(self, message: str = "Project not found"):
        self.message = message
        self.error_code = error_codes.project_not_found


class ServiceCreationFailedException(XprExceptions):
    """
    Thrown when kubernetes api fails to create a service
    """

    def __init__(self, message: str = "Service creation failed"):
        self.message = message
        self.error_code = error_codes.service_creation_failed


class PortPatchingAttemptedException(XprExceptions):
    """
    Thrown when user attempts editing a deployed port's specs
    """

    def __init__(self, message: str = "Port patching attempted"):
        self.message = message
        self.error_code = error_codes.port_patching_attempted


class NamespaceCreationFailedException(XprExceptions):
    """
    Thrown when kubernetes api fails to create a namespace
    """

    def __init__(self, message: str = "Namespace creation failed"):
        self.message = message
        self.error_code = error_codes.namespace_creation_failed


class JobCreationFailedException(XprExceptions):
    """
    Thrown when kubernetes api fails to create a job
    """

    def __init__(self, message: str = "Job creation failed"):
        self.message = message
        self.error_code = error_codes.job_creation_failed


class CronjobCreationFailedException(XprExceptions):
    """
    Thrown when kubernetes api fails to create a cronjob
    """

    def __init__(self, message: str = "Cronjob creation failed"):
        self.message = message
        self.error_code = error_codes.cronjob_creation_failed


class ProjectDeploymentFailedException(XprExceptions):
    """
    Thrown when a project fails to deploy on Kubernetes
    """

    def __init__(self, message: str = "Project deployment failed"):
        self.message = message
        self.error_code = error_codes.project_deployment_failed


class ProjectUndeploymentFailedException(XprExceptions):
    """
    Thrown when a project fails to undeploy from Kubernetes
    """

    def __init__(self, message: str = "Project undeployment failed"):
        self.message = message
        self.error_code = error_codes.project_undeployment_failed


class InvalidJobTypeException(XprExceptions):
    """
    Thrown when user specifies an invalid or empty job type
    """

    def __init__(self, message: str = "Invalid job type"):
        self.message = message
        self.error_code = error_codes.invalid_job_type


class InvalidJobCommandsException(XprExceptions):
    """
    Thrown when user specifies invalid or empty job commands
    """

    def __init__(self, message: str = "Invalid job command"):
        self.message = message
        self.error_code = error_codes.invalid_job_commands


class ProjectDeveloperException(XprExceptions):
    """
    This exception is called if the developers information is incorrect
    """

    def __init__(self, message: str = "Invalid developer info"):
        self.message = message
        self.error_code = error_codes.invalid_developer_information


class ProjectPipelinesException(XprExceptions):
    """
    This exception is called when the pipelines info provided is incorrect
    """

    def __init__(self, message: str = "Pipeline default "
                                      "keys specified incorrectly"):
        self.message = message
        self.error_code = error_codes.incorrect_pipelines_information


class ProjectSetupException(XprExceptions):
    """
    This exception is called while facing any issue in the project setup
    """

    def __init__(self, message: str = "Project Setup Failed"):
        self.message = message
        self.error_code = error_codes.project_setup_failed


class SkeletonCodeException(XprExceptions):
    """
    This exception is called if there is any issue in setting skeleton repo
    """

    def __init__(self, message: str = "Skeletong repo creationg failed"):
        self.message = message
        self.error_code = error_codes.skeleton_repo_creation_failed


class BitbucketCloneException(XprExceptions):
    """
    This exception is called when there is any issue in cloning a repo
    """

    def __init__(self, message: str = "Repo clone failed"):
        self.message = message
        self.error_code = error_codes.repo_clone_failed


class ProjectDeactivationException(XprExceptions):
    """
    This exception occurs if there is any error while deactivating project
    """

    def __init__(self, message: str = "Project deactivation failed"):
        self.message = message
        self.error_code = error_codes.project_deactivation_failed


class NoProjectException(XprExceptions):
    """
    This exception is called if there is any error in modifying project
    """

    def __init__(self, message: str = "Project not created"):
        self.message = message
        self.error_code = error_codes.project_not_created


class InvalidCronScheduleException(XprExceptions):
    """
    Thrown when user provides an invalid cron job schedule
    """

    def __init__(self, message: str = "Invalid cron schedule"):
        self.message = message
        self.error_code = error_codes.invalid_cron_schedule


class DeploymentCreationFailedException(XprExceptions):
    """
    Thrown when kubernetes api fails to create a deployment
    """

    def __init__(self, message: str = "Deployment creationg failed"):
        self.message = message
        self.error_code = error_codes.deployment_creation_failed


class IncompleteUserInfoException(XprExceptions):
    """
    This exception is called when provided user info is not complete
    """

    def __init__(self, message: str = "Incomplete user information"):
        self.message = message
        self.error_code = error_codes.incomplete_user_information


class PasswordStrengthException(XprExceptions):
    """
    This exception is called when the password strength is not enough
    """

    def __init__(self, message: str = "Password not valid"):
        self.message = message
        self.error_code = error_codes.password_not_valid


class IncompleteProvisionInfoException(XprExceptions):
    """
    This exception is called when the provision info is incomplete
    """

    def __init__(self, message: str = "Incomplete provision info"):
        self.message = message
        self.error_code = error_codes.incomplete_provision_information


class InvalidProvisionInfoException(XprExceptions):
    """
    This exception is called when the provision info is invalid
    """

    def __init__(self, message: str = "Invalid provision info"):
        self.message = message
        self.error_code = error_codes.invalid_provision_information


class NodeNotFoundException(XprExceptions):
    """
    This exception is called when the node is not found
    """

    def __init__(self, message: str = "Node not found"):
        self.message = message
        self.error_code = error_codes.node_not_found


class NodeReProvisionException(XprExceptions):
    """
    This exception is called when the node is already provisioned
    """

    def __init__(self, message: str = "Node already provisioned"):
        self.message = message
        self.error_code = error_codes.node_already_provisioned


class InvalidMasterException(XprExceptions):
    """
    This exception is called when the master node info is invalid
    """

    def __init__(self, message: str = "Invalid master node"):
        self.message = message
        self.error_code = error_codes.invalid_master_node


class MasterNotProvisionedException(XprExceptions):
    """
    This exception is called when the master node is not provisioned
    """

    def __init__(self, message: str = "Master not provisioned"):
        self.message = message
        self.error_code = error_codes.master_not_provisioned


class ProvisionKubernetesException(XprExceptions):
    """
    This exception is called when the node provision is failed
    because of kubernetes error
    """

    def __init__(self, message: str = "Kubernetes error"):
        self.message = message
        self.error_code = error_codes.kubernetes_error


class NodeDeactivatedException(XprExceptions):
    """
    This exception is called when the node is already deactivated
    """

    def __init__(self, message: str = "Node already deactivated"):
        self.message = message
        self.error_code = error_codes.node_already_deactivated


class IncompleteNodeInfoException(XprExceptions):
    """
    This exception is called when the Node information is incomplete
    """

    def __init__(self, message: str = "Incomplet node information"):
        self.message = message
        self.error_code = error_codes.incomplete_node_information


class UnProvisionedNodeException(XprExceptions):
    """
    This exception is called when the node is not provisioned
    """

    def __init__(self, message: str = "Node not provisioned"):
        self.message = message
        self.error_code = error_codes.node_not_provisioned


class NodeTypeException(XprExceptions):
    """
    This exception is called when the node type is invalid
    """

    def __init__(self, message: str = "Invalid node type"):
        self.message = message
        self.error_code = error_codes.invalid_node_type


class NodeAlreadyAssignedException(XprExceptions):
    """
    This exception is called when the node is already assigned
    """

    def __init__(self, message: str = "Node already assigned"):
        self.message = message
        self.error_code = error_codes.node_already_assigned


class NodeAssignException(XprExceptions):
    """
    This exception is called when the node assignation failed
    """

    def __init__(self, message: str = "Node assignment failed"):
        self.message = message
        self.error_code = error_codes.node_assign_failed


class CallDeactivateNodeException(XprExceptions):
    """
    This exception is called when the node assignation failed
    """

    def __init__(self, message: str = None):
        self.message = message
        self.error_code = error_codes.call_deactivate_node


class FileNotFoundException(XprExceptions):
    """
    This exception is called when the file is not present at the path
    """

    def __init__(self, message: str = None):
        self.message = message
        self.error_code = error_codes.file_not_found


class JsonLoadError(XprExceptions):
    """
    This exception is called when the file is not present at the path
    """

    def __init__(self, message: str = None):
        self.message = message
        self.error_code = error_codes.json_load_error


class NodeDeletionKubernetesException(XprExceptions):
    """
    This exception is called when the node deletion failed
    because of kubernetes error
    """

    def __init__(self, message: str = "Node Deletion failed"):
        self.message = message
        self.error_code = error_codes.kubernetes_error


class IllegalModificationException(XprExceptions):
    """
    Class to define an exception thrown when a field is attempted to be
    modified illegally
    """

    def __init__(self, message: str = "Illegal modification"):
        self.message = message
        self.error_code = error_codes.cannot_modify_password


class InvalidPasswordException(XprExceptions):
    """
    Modification is not correct
    """

    def __init__(self, message: str = "Invalid password"):
        self.message = message
        self.error_code = error_codes.password_not_valid


class IncorrectDeclarativeJSONDefinitionException(XprExceptions):
    """
    Thrown when the provided declarative JSON for Kubeflow pipeline is
    incorrectly defined
    """

    def __init__(self, message: str = "Declarative JSON defined incorrectly"):
        self.message = message
        self.error_code = error_codes.declarative_json_incorrect


class ReferenceNotFoundException(XprExceptions):
    """
    Thrown when an object being referred is not found in declarative json
    """

    def __init__(self, message):
        self.message = message
        self.error_code = error_codes.reference_not_found


class PipelineNotFoundException(XprExceptions):
    """
    Thrown when the pipeline in question is not found in the database
    """

    def __init__(self, message: str = "Pipeline doesn't exist."):
        self.message = message
        self.error_code = error_codes.pipeline_not_found


class AmbassadorPortFetchException(XprExceptions):
    """
    Thrown when ambassador port patching fails
    """

    def __init__(self, message: str = "Ambassador port fetching failed."):
        self.message = message
        self.error_code = error_codes.ambassador_port_fetching_failed


class PipelineUploadFailedException(XprExceptions):
    """
    thrown when pipeline upload via API fails
    """

    def __init__(self, message: str = "Pipeline upload failed."):
        self.message = message
        self.error_code = error_codes.pipeline_upload_failed


class InvalidDatatypeException(XprExceptions):
    """
    Invalid Datatype Provided
    """

    def __init__(self, message: str = "Invalid Datatype Passed"):
        self.message = message


class EmailException(XprExceptions):
    """
    Unable to send the email notification
    """

    def __init__(self, message: str = "Unable to send"):
        self.message = message
        self.error_code = error_codes.password_not_valid


class APIGatewayExceptions(XprExceptions):
    """
    Modification is not correct
    """

    def __init__(self, message: str = "Gateway is not working as expected"):
        self.message = message
        self.error_code = error_codes.gateway_connection_error


class APIGatewayDuplicateExceptions(XprExceptions):
    """
    Same services or route exists
    """

    def __init__(self, message: str = "Duplicate entry in the gateway"):
        self.message = message
        self.error_code = error_codes.gateway_connection_error


class InvalidArgumentException(XprExceptions):
    """
    This exception is thrown when the provided Arguments does not contain
    expected data or is of invalid type
    """


class InvalidConfigException(XprExceptions):
    """
    This exception is thrown when the config file is invalid
    """


class CommandExecutionFailedException(XprExceptions):
    """
    This exception is thrown when command execution failed
    """


class PackageFailedException(XprExceptions):
    """
    This exception is thrown when package is not exited successfully while
    performing a the task
    """


class JenkinsConnectionFailedException(XprExceptions):
    """
    This exception is thrown when Jenkins connection is failed
    """


class JenkinsInvalidInputException(XprExceptions):
    """
    This exception is thrown when Jenkins connection is failed
    """


class SerializationFailedException(XprExceptions):
    """ Raised when serialization failed for an object"""

    def __init__(self, message: str = "Serialization Failed"):
        self.message = message
        self.error_code = error_codes.serialization_failed


class DeserializationFailedException(XprExceptions):
    """ Raised when deserialization failed for an object"""

    def __init__(self, message: str = "Deserialization Failed"):
        self.message = message
        self.error_code = error_codes.deserialization_failed


class InvalidEnvironmentException(XprExceptions):
    """
    invalid environment specified for project deployment
    """

    def __init__(self, message: str = "Invalid environment specified"):
        self.message = message
        self.error_code = error_codes.invalid_environment_error


class NoClustersPresentException(XprExceptions):
    """
    no clusters present for environment allocation
    """

    def __init__(self,
                 message: str = "No clusters present for environment allocation"):
        self.message = message
        self.error_code = error_codes.no_clusters_present_error


class IncorrectDeploymentException(XprExceptions):
    """
    attempt to deploy project to higher environment before deploying to all
    possible lower environments
    """

    def __init__(self,
                 message: str = "Attempt to deploy project to higher "
                                "environment before deploying to all "
                                "possible lower environments"):
        self.message = message
        self.error_code = error_codes.incorrect_deployment_error


class RepoNotProvidedException(XprExceptions):
    """
    Repo Name is not provided
    """

    def __init__(self, message: str = "Repo name not provided"):
        self.message = message
        self.error_code = error_codes.pachyderm_repo_not_provided


class DatasetInfoException(XprExceptions):
    """
    Dataset info provided is incomplete or invalid
    """

    def __init__(self, message):
        self.message = message
        self.error_code = error_codes.dataset_info_error


class DatasetPathException(XprExceptions):
    """
    Dataset path is invalid or not found
    """

    def __init__(self, message):
        self.message = message
        self.error_code = error_codes.dataset_path_invalid


class BranchInfoException(XprExceptions):
    """
    occurs when branch info is invalid/incomplete
    """

    def __init__(self, message):
        self.message = message
        self.error_code = error_codes.pachyderm_branch_info_error


class PachydermFieldsNameException(XprExceptions):
    """
    occurs when name of any key fields of pachyderm does not follow pattern
    """

    def __init__(self,
                 message="Name of repo, branch and dataset can only be"
                         "string of alphanumeric characters, underscores"
                         "or dashes"):
        self.message = message
        self.error_code = error_codes.pachyderm_field_name_error


class PachydermOperationException(XprExceptions):
    """
    occurs when any pachyderm operation fails
    """

    def __init__(self, message):
        self.message = message
        self.error_code = error_codes.pachyderm_operation_error


class LocalFilePathException(XprExceptions):
    """
    occurs when there is any exception with local path
    """

    def __init__(self, message):
        self.message = message
        self.error_code = error_codes.local_path_exception