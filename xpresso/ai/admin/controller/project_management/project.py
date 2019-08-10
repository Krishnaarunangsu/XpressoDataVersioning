import json
import os
from copy import deepcopy
from xpresso.ai.admin.controller.xprobject import XprObject
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.project_management.kubeflow.declarative_pipeline.declarative_pipeline_builder \
    import DeclarativePipelineBuilder


class Project(XprObject):
    """
    This class represents a User
    """
    project_config_path = "/opt/xpresso.ai/config/project_config.json"
    with open(project_config_path, "r", encoding="utf-8") as config_file:
        try:
            project_config = json.load(config_file)
        except (FileNotFoundError, json.JSONDecodeError):
            raise ProjectConfigException

    def __init__(self, project_json=None):
        self.logger = XprLogger()
        """
        Constructor:
        """
        self.logger.debug(f"Project constructor called with {project_json}")
        super().__init__(project_json)

        # List of all the fields project can contain
        self.complete_fields = self.project_config["project_key_list"]
        # These are mandatory fields that needs to be provided in user_json
        self.mandatory_fields = ["name", "description", "owner"]

        # primaryRole of a user has to one of these
        self.valid_values = {"primaryRole": ["Dev", "PM", "DH", "Admin", "Su"]}

        # fields that cannot be modified
        self.unmodifiable_fields = ["name"]

        # fields that should be displayed in the output
        self.display_fields = self.project_config["project_output_fields"]
        self.logger.debug("User constructed successfully")

    def check_fields(self) -> int:
        """
            checks all the input fields in project information json

            checks if the provided fields type and format is same and in
            accordance with project format.
            Checks if the mandatory fields are provided.
        """
        # checks if the type of the input fields is valid
        for (field, field_type) in self.project_config["project_field_types"].items():
            if field in self.data:
                if type(self.data[field]).__name__ != field_type:
                    raise ProjectFieldsException(
                        f"type of '{field}' in project info is invalid"
                    )

        # checks if the mandatory fields are provided and if they are not empty
        for (field, field_flag) in self.project_config["project_field_flags"].items():
            if field_flag:
                if field in self.data and len(self.data[field]):
                    continue
                else:
                    raise IncompleteProjectInfoException()
        return 200

    @staticmethod
    def check_duplicate_component(component_list):
        """
            checks if any component is specified/used more than once
        """
        component_names = []
        for component in component_list:
            if component["name"] not in component_names:
                component_names.append(component["name"])
            else:
                component_name = component["name"]
                raise DuplicateComponentException(
                    f"Component/Pipeline with name '{component_name}' "
                    f"already exists in this project"
                )

    def component_info_check(self, component_list):
        """
            checks if the components are specified correctly

            checks if the type, flavor and name for each of the component
            is specified correctly in input json
        """
        component_field_set = self.project_config["component_fields"]
        for component in component_list:
            # check if each of type, name & flavor are specified correctly
            for (key, val) in component.items():
                # checks if only allowed keys are present in the component spec
                if key not in component_field_set:
                    raise ComponentFieldException(
                        f"'{key}' field not specified in component information"
                    )
                elif type(val).__name__ != component_field_set[key]:
                    raise ComponentFieldException(
                        f"Invalid field type of '{key}'"
                    )
                elif not len(val):
                    raise ComponentFieldException(f"'{key}' field is empty")

            # checks if all the mandatory fields are provided in the components
            for field in component_field_set:
                if field not in component:
                    raise ComponentFieldException(
                        f"'{field}' is not in the allowed fields of components"
                    )

            temp_type = component["type"]
            temp_flavor = component["flavor"]
            if temp_type not in self.project_config["component_flavors"]:
                raise ComponentFieldException(
                    f"Component type '{temp_type}' is not valid"
                )
            elif temp_flavor not in\
                    self.project_config["component_flavors"][temp_type]:
                raise ComponentFieldException(
                    f"'{temp_flavor}' flavor unavailable for {temp_type}"
                )

    def check_owner(self, persistence_manager):
        if 'uid' not in self.data["owner"]:
            raise ProjectOwnerException("Owner uid needs to be provided")
        elif not len(self.data["owner"]["uid"]):
            raise ProjectOwnerException("Owner uid is empty")
        else:
            uid_json = {'uid': self.data["owner"]["uid"]}
        owner = persistence_manager.find("users", uid_json)
        if not len(owner):
            raise ProjectOwnerException(
                "User not found with provided owner uid"
            )

    def check_developers(self, persistence_manager, devs=None):
        if not devs:
            developers = self.data["developers"]
        else:
            developers = devs
        for dev_name in developers:
            if type(dev_name).__name__ != 'str':
                raise ProjectDeveloperException(
                    f"Developer name should be string"
                )
            if not len(dev_name):
                raise ProjectDeveloperException(
                    "Developer name should not be empty"
                )
            else:
                developer = persistence_manager.find(
                    "users", {"uid": dev_name}
                    )
                if not len(developer):
                    raise ProjectDeveloperException(
                        f"Developer {dev_name} not found"
                    )

    def check_pipelines(self, pipelines, persistence_manager,
                        project_components):
        """
        validates the pipeline format provided and checks for consistency,
        duplicity.
        Args:
            pipelines: input pipeline info
            persistence_manager: persistence manager
            project_components : list of names of project components

        Returns:

        """
        default_pipeline_keys = self.project_config['pipeline_fields'].keys()
        keys_without_version = list(self.project_config['pipeline_fields'].keys())
        keys_without_version.remove('deploy_version_id')
        declarative_pipeline_builder = DeclarativePipelineBuilder(
            persistence_manager)
        for pipeline in pipelines:
            if set(default_pipeline_keys) != set(pipeline.keys()) and \
                    set(keys_without_version) != set(pipeline.keys()):
                self.logger.error("default keys defined incorrectly")
                raise ProjectPipelinesException
            for component in pipeline['components']:
                if component not in project_components:
                    self.logger.error('pipeline component not found in '
                                      'project components')
                    raise ComponentsSpecifiedIncorrectlyException(
                        f'Pipeline component "{component}" not '
                        f'found in project components.')
            declarative_pipeline_builder.prevalidate_declarative_json(
                pipeline['declarative_json'])
        self.check_duplicate_component(pipelines)

    def project_info_check(self, persistence_manager):
        """
        checks if the information provided for project creation is valid

        checks all the mandatory fields and its format. checks the
        components, developers and owner information.
        """
        keys = self.data.keys()
        # checks if the input fields provided are valid and present
        # support case type mistakes in next release
        for key in keys:
            if key not in self.project_config["project_key_list"]:
                raise ProjectFormatException(
                    f"Invalid field '{key}' in project json"
                    )

        # checks if all fields are specified correctly
        self.check_fields()
        # checks if the components are provided with info and validates format
        if "components" in self.data:
            self.component_info_check(self.data["components"])
            self.check_duplicate_component(self.data["components"])

        self.check_owner(persistence_manager)
        if "pipelines" in self.data:
            project_component_names = \
                [component['name'] for component in self.data['components']]
            self.check_pipelines(self.data['pipelines'], persistence_manager,
                                 project_component_names)
        # checks the developers format is valid if specified
        if "developers" in self.data and len(self.data["developers"]):
            self.check_developers(persistence_manager)

    def complete_project_info(self):
        out_json = deepcopy(self.project_config["sample_json"])
        name = self.data["name"]
        for key, val in self.data.items():
            if key != "components":
                out_json[key] = val
        if "components" in self.data:
            for component in self.data["components"]:
                component_name = component["name"]
                out_component = deepcopy(self.project_config["sample_component"])
                for key, val in component.items():
                    if key in self.project_config["sample_component"]:
                        out_component[key] = val
                out_component["dockerPrefix"] = (
                    f"dockerregistry.xpresso.ai/xprops/{name}/{component_name}--"
                )
                out_json["components"].append(out_component)
        self.data = out_json
        if "pipelines" in self.data:
            for pipeline in self.data["pipelines"]:
                if "deploy_version_id" not in pipeline.keys():
                    pipeline['deploy_version_id'] = 1

    def modify_info_check(self, changes_json, persistence_manager):
        # if not self.data["activationStatus"]:
        #     if "activationStatus" not in changes_json:
        #         return error_codes.activate_project_first
        #     elif not changes_json["activationStatus"]:
        #         return error_codes.activate_project_first
        #     else:
        #         changes_json["activationStatus"] = True
        if "activationStatus" in changes_json and \
                not changes_json["activationStatus"]:
            raise ProjectNotFoundException("Project is currently not active.")

        keys = changes_json.keys()
        # support case type mistakes in next release
        for key in keys:
            if key not in self.project_config["project_key_list"]:
                raise ProjectFieldsException(
                    f"Invalid field '{key}' provided for modify_project"
                )

        for (field, field_type) in\
                self.project_config["project_field_types"].items():
            if field in changes_json:
                if type(changes_json[field]).__name__ != field_type:
                    raise ProjectFieldsException(
                        f"Invalid type of field '{field}'"
                    )
        if "owner" in changes_json and len(changes_json["owner"]):
            self.check_owner(changes_json["owner"])

        if "developers" in changes_json and len(changes_json["developers"]):
            self.check_developers(
                persistence_manager, devs=changes_json["developers"]
                )

        if "components" in changes_json and len(changes_json["components"]):
            old_components = self.data["components"]
            new_components = changes_json["components"]
            self.component_info_check(new_components)
            self.check_duplicate_component(old_components + new_components)

        if "pipelines" in changes_json and len(changes_json["pipelines"]):
            new_pipeline = changes_json['pipelines']
            project_components = \
                [component['name'] for component in self.data['components']]
            if "components" in changes_json.keys():
                for component in changes_json['components']:
                    project_components.append(component['name'])
            self.check_pipelines(new_pipeline,
                                 persistence_manager, project_components)

    def modify_project_info(self, changes_json):
        name = self.data["name"]
        out_json = deepcopy(changes_json)
        out_json["components"] = self.data["components"]
        if "components" in changes_json:
            for component in changes_json["components"]:
                component_name = component["name"]
                out_component = deepcopy(self.project_config["sample_component"])
                for key, val in component.items():
                    if key in self.project_config["sample_component"]:
                        out_component[key] = val
                out_component["dockerPrefix"] = (
                    f"dockerregistry.xpresso.ai/xprops/{name}/{component_name}--"
                )
                out_json["components"].append(out_component)
        if 'pipelines' in changes_json:
            for pipeline in changes_json['pipelines']:
                if "deploy_version_id" not in pipeline.keys():
                    pipeline['deploy_version_id'] = 1
            if 'pipelines' not in self.data.keys():
                out_json['pipelines'] = changes_json['pipelines']
            else:
                out_json['pipelines'] = changes_json['pipelines'] + \
                                        self.data['pipelines']

        return out_json
