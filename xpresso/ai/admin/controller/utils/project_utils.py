import simplejson as json
import re
from copy import deepcopy
from croniter import croniter
from pymongo import MongoClient
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.utils import error_codes
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.persistence.mongopersistencemanager import MongoPersistenceManager
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import UnsuccessfulConnectionException \
    as db_connection_failure
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    UnsuccessfulOperationException as db_operation_failure

logger = XprLogger()

# loads the project configuration file
project_config_path = "/opt/xpresso.ai/config/project_config.json"
with open(project_config_path, "r", encoding="utf-8") as config_file:
    try:
        project_config = json.load(config_file)
    except (FileNotFoundError, json.JSONDecodeError):

        project_config = {}


def connect_to_db():
    """
        connects to persistence

        connects to persistence for persistence operations to validate owner and developer
        information

        Returns
        -------
            returns a pymongo persistence client
    """
    mongo_section = "mongodb"
    url = "mongo_url"
    db = "database"
    uid = "mongo_uid"
    pwd = "mongo_pwd"
    write = 'w'

    config_path = XprConfigParser.DEFAULT_CONFIG_PATH
    config = XprConfigParser(config_path)
    # add exception
    db_client = MongoPersistenceManager(url=config[mongo_section][url],
                      db=config[mongo_section][db],
                      uid=config[mongo_section][uid],
                      pwd=config[mongo_section][pwd],
                      w=config[mongo_section][write]
                      )
    return db_client


def disconnect_from_db(db_client):
    """
        disconnects from persistence

        disconnects the input mongo client from mongo persistence

        Paremeters
        ---------
            db_client : mongo client connected to a specified persistence
    """
    url = "mongo_url"
    write = "w"
    mongo_client = MongoClient(host=url, w=write)
    db_client.disconnect(mongo_client)


def project_info_check(project_json: dict) -> int:
    """
        checks if the information provided for project creation is valid

        checks all the mandatory fields and its format. checks the components,
        developers and owner information.


        Parameters
        ----------
            project_json : dict of project information

        Returns
        -------
            status_code : returns error_code in case of information failure
                          else returns 200 as success_code
    """
    try:
        keys = project_json.keys()
        # checks if the input fields provided are valid and present in persistence
        # support case type mistakes in next release
        for key in keys:
            if key not in project_config["project_key_list"]:
                return error_codes.invalid_project_format

        # checks if all fields are specified correctly
        field_check = check_fields(project_json)
        if field_check != 200:
            return field_check

        # checks if the components are provided with info and validates format
        if "components" in project_json:
            component_status = component_info_check(project_json["components"])
            if component_status != 200:
                return component_status
            elif not check_duplicate_component(project_json["components"]):
                return error_codes.component_already_exists

        owner_status = check_owner(project_json["owner"])
        if owner_status != 200:
            return owner_status

        # checks the developers format is valid if specified
        if "developers" in project_json and len(project_json["developers"]):
            dev_status = check_developers(project_json["developers"])
            if dev_status != 200:
                return dev_status
        return 200

    except (KeyError, db_connection_failure, db_operation_failure) as info_err:
        logger.error(f"\nError in checking information :\n{info_err}")
        return error_codes.internal_config_error


def check_fields(project_json: dict) -> int:
    """
        checks all the input fields in project information json

        checks if the provided fields type and format is same and in accordance
        with project format. Checks if the mandatory fields are provided.

        Parameters
        ----------
            project_json : dict of project information
        Returns
        -------
            status_code : returns error_code in case of information failure
                          else returns 200 as success_code
    """
    # checks if the type of the input fields is valid
    # project_config["field_types"] has map of valid types to input fields
    for (field, field_type) in project_config["project_field_types"].items():
        if field in project_json:
            if type(project_json[field]).__name__ != field_type:
                return error_codes.invalid_project_field_format

    # checks if the mandatory fields are provided and if they are not empty
    for (field, field_flag) in project_config["project_field_flags"].items():
        if field_flag:
            if field in project_json and len(project_json[field]):
                continue
            else:
                return error_codes.incomplete_project_information

    return 200


def check_owner(owner_info):
    if 'uid' in owner_info and len(owner_info['uid']):
        uid_json = {'uid': owner_info['uid']}
    else:
        return error_codes.invalid_owner_information
    db_client = connect_to_db()
    owner = db_client.find("users", uid_json)
    # disconnect_from_db(db_client)
    if not len(owner):
        return error_codes.invalid_owner_information

    return 200


def check_developers(dev_info):
    
    db_client = connect_to_db()
    for dev in dev_info:
        if type(dev).__name__ == 'str' and len(dev):
            developer = db_client.find("users", {"uid": dev})
            if not len(developer):
                # disconnect_from_db(db_client)
                return error_codes.developer_not_found
        else:
            # disconnect_from_db(db_client)
            return error_codes.invalid_developer_information
    # disconnect_from_db(db_client)
    return 200


def complete_project_info(self):
    try:
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
    except KeyError:
        return error_codes.internal_config_error


def filter_project_output(projects):
    filtered_projects = []
    for project in projects:
        new_project = {}
        for field in project_config["project_output_fields"]:
            if field in project:
                new_project[field] = project[field]
        filtered_projects.append(new_project)
    return filtered_projects


def check_duplicate_component(component_list):
    """
        checks if any component is specified more than once

        checks if any component name is used more than once.

        Parameters
        ----------
            component_list: list of all the components

        Returns
        ------
            returns success_code
    """
    component_names = []
    for component in component_list:
        if component["name"] not in component_names:
            component_names.append(component["name"])
        else:
            return False
    return True


def modify_info_check(project_info, changes_json):
    try:
        if not project_info["activationStatus"]:
            if "activationStatus" not in changes_json:
                return error_codes.activate_project_first
            elif not changes_json["activationStatus"]:
                return error_codes.activate_project_first
            else:
                changes_json["activationStatus"] = True
        if "activationStatus" in changes_json and\
                not changes_json["activationStatus"]:
            return error_codes.call_deactivate_method

        keys = changes_json.keys()
        # support case type mistakes in next release
        for key in keys:
            if key not in project_config["project_key_list"]:
                return error_codes.invalid_project_format

        for (field, field_type) in project_config["project_field_types"].items():
            if field in changes_json:
                if type(changes_json[field]).__name__ != field_type:
                    return error_codes.invalid_project_field_format

        if "owner" in changes_json and len(changes_json["owner"]):
            owner_status = check_owner(changes_json["owner"])
            if owner_status != 200:
                return owner_status

        if "developers" in changes_json and len(changes_json["developers"]):
            dev_status = check_developers(changes_json["developers"])
            if dev_status != 200:
                return dev_status

        if "components" in changes_json and len(changes_json["components"]):
            old_components = project_info["components"]
            new_components = changes_json["components"]
            component_status = component_info_check(new_components)
            if component_status != 200:
                return component_status
            elif not check_duplicate_component(
                    old_components+new_components
                    ):
                return error_codes.component_already_exists

        return 200
    except (KeyError, db_connection_failure, db_operation_failure) as info_err:
        logger.error(f"\nError in checking information :\n{info_err}")
        return error_codes.internal_config_error


def component_info_check(components):
    """
        checks if the components are specified correctly

        checks if the type, flavor and name for each of the component
        is specified correctly in input json
    """
    component_field_set = project_config["component_fields"]
    for component in components:
        # check if each of type, name & flavor are specified correctly
        for (key, val) in component.items():
            # checks if only allowed keys are present in the component spec
            if key not in component_field_set:
                return error_codes.unknown_key_component
            elif type(val).__name__ != component_field_set[key]:
                return error_codes.incorrect_component_fields
            elif not len(val):
                return error_codes.incorrect_component_fields

        # checks if all the mandatory fields are provided in the component info
        for field in component_field_set:
            if field not in component:
                return error_codes.invalid_component_format

        temp_type = component["type"]
        temp_flavor = component["flavor"]
        if temp_type in project_config["component_flavors"] and\
                temp_flavor in project_config["component_flavors"][temp_type]:
            continue
        else:
            return error_codes.invalid_component_format
    return 200


def modify_project_info(project_info, changes_json):
    name = project_info["name"]
    out_json = deepcopy(changes_json)
    out_json["components"] = project_info["components"]
    if "components" in changes_json:
        for component in changes_json["components"]:
            component_name = component["name"]
            out_component = deepcopy(project_config["sample_component"])
            for key, val in component.items():
                if key in project_config["sample_component"]:
                    out_component[key] = val
            out_component["dockerPrefix"] = (
                f"dockerregistry.xpresso.ai/xprops/{name}/{component_name}--"
            )
            out_json["components"].append(out_component)
    return out_json


def clean_project():
    pass


def modify_string_for_deployment(input_string):
    """
    Converts all special characters in a string to "-" .
    Args:
        input_string: string to be fixed

    Returns: fixed string

    """

    fixed_string = re.sub("[!@#$%^&*/\<>?|`~=_+]", "-", input_string)
    return fixed_string


def validate_cronjob_format(schedule):
    """
    validates if the cronjob schedule provided is valid
    :param schedule: input schedule
    :return: True/False
    """
    return croniter.is_valid(schedule)


def extract_component_image(db_components_info, input_components):
    """
    extracts the docker image corresponding to build version for given
    components.
    Args:
        db_components_info: info of all components of the project obtained
        from database
        input_components: info of components to be deployed obtained from
        user input
    Returns: dict of components and corresponding image
    """
    result = {}
    pipeline_components = input_components.keys()
    for component in pipeline_components:
        if 'build_version' not in input_components[component].keys():
            logger.error('Build version not specified in input json.')
            raise InvalidBuildVersionException(
                f'Build version not specified in input json for '
                f'component "{component}".')
        for project_comp in db_components_info:
            if project_comp['name'] == component:
                build_flag = False
                for index, version in enumerate(project_comp['versions']):
                    if version['version_id'] == \
                            input_components[project_comp['name']][
                                'build_version']:
                        result[f'{component}'] = version['dockerImage']
                        build_flag = True
                        break
                if not build_flag:
                    logger.error(
                        f"Invalid build version specified "
                        f"for component '{project_comp['name']}'")
                    raise InvalidBuildVersionException(
                        f"Invalid build version specified for component "
                        f"'{project_comp['name']}'")
                break
    return result
