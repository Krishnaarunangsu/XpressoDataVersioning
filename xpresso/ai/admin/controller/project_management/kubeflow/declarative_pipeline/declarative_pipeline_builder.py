__all__ = ['DeclarativePipelineBuilder']
__author__ = 'Sahil Malav'


import json
import yaml
import os
from copy import deepcopy
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.utils.constants import *
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.admin.controller.project_management.kubeflow.kubeflow_utils \
    import KubeflowUtils


class DeclarativePipelineBuilder:

    # all the pipeline reference variables will be stored in this array as
    # they are discovered by the code so that we can check for any faulty
    # reference made which is not present in the reference array
    reference_array = []

    def __init__(self, persistence_manager):
        self.kubeflow_utils = KubeflowUtils(persistence_manager)
        self.logger = XprLogger()
        self.executor = LocalShellExecutor()
        config_path = XprConfigParser.DEFAULT_CONFIG_PATH
        self.config = XprConfigParser(config_path)
        self.declarative_pipeline_folder = self.config[
            PROJECTS_SECTION][DECLARATIVE_PIPELINE_FOLDER]
        self.content = self.declarative_pipeline_folder_check()

    def declarative_pipeline_folder_check(self):
        """
        checks whether declarative pipeline folder is present
        Returns: contents of template

        """
        if not os.path.isdir(self.declarative_pipeline_folder):
            os.makedirs(self.declarative_pipeline_folder,
                        permission_755)
        kubeflow_template = self.config[PROJECTS_SECTION][
            KUBEFLOW_TEMPLATE]
        try:
            with open(kubeflow_template, 'r') as f:
                template_content = f.read()
                return template_content
        except FileNotFoundException:
            self.logger.error('kubeflow template file not found')

    def prevalidate_declarative_json(self, pipeline_info):
        """
        Validates (with dummy data) if the pipeline yaml file is being created
        properly before adding pipeline as a part of project.
        Args:
            pipeline_info: declarative JSON file

        Returns: nothing

        """
        self.logger.info('entering prevalidate_declarative_json')
        temp_component_images = {}
        self.logger.info('creating dict with temporary component images')
        for component in pipeline_info['components']:
            self.validate_component_keys(component.keys())
            temp_component_images[component['xpresso_reference']] = "temp_image"
        self.generate_pipeline_file(pipeline_info, temp_component_images, 0)
        self.logger.info('Pipeline validated.')

    def check_for_reference(self, value):
        """
        Checks if the provided value has any faulty reference.
        Args:
            value: value to be checked

        Returns: raises exception if reference is not found

        """
        self.logger.info(f'entering check_for_reference to '
                         f'validate {value}')
        if '.output' in value:
            reference = value.split('.')[0]
            self.check_for_reference(reference)
            if open_parenthesis in reference:
                # in case of typecasting
                reference = reference.split(open_parenthesis)[1]
            if reference not in self.reference_array:
                self.logger.error(f'Reference "{reference}" not found.')
                raise ReferenceNotFoundException(f'Reference "{reference}" not '
                                                 f'found in declarative JSON')
        self.logger.info('Reference validated. Exiting.')

    def modify_for_function_parameters(self, func_params):
        """
        modifies a string (json key-value pair) to be used as a function's
        parameters
        Args:
            func_params: json key-value pair string (in xpresso defined format)

        Returns: modified string, fit for using as a function's parameters

        """
        self.logger.info('entering modify_for_function_parameters')
        param_list = []
        for key, value in func_params.items():
            modified_key = key.replace(variable_indicator, "")
            if variable_indicator not in str(value):
                if double_quote in value:
                    value = value.replace(double_quote, escape_quote)
                modified_value = f'"{value}"'
            else:
                modified_value = value.replace(variable_indicator, "")
            # check for any faulty reference
            self.check_for_reference(modified_value)
            param_list.append(f'{modified_key}={modified_value}')
            self.reference_array.append(modified_key)
        result = ', '.join(param_list)
        self.logger.info(f'exiting modify_for_function_parameters with '
                         f'output {result}')
        return result

    def modify_for_function_variables(self, func_vars):
        """
        modifies a string (json key-value pair) to be used as a function's
        variables
        Args:
            func_vars: json key-value pair string (in xpresso defined format)

        Returns: modified string, fit for use as a function's variables

        """
        self.logger.info('entering modify_for_function_variables')
        result = """"""
        for key, value in func_vars.items():
            modified_key = key.replace(variable_indicator, "")
            if variable_indicator not in value:
                if double_quote in value:
                    value = value.replace(double_quote, escape_quote)
                modified_value = f'"{value}"'
            else:
                modified_value = value.replace(variable_indicator, "")
            # check for any faulty reference
            self.check_for_reference(modified_value)
            self.reference_array.append(modified_key)
            result = result + f'{modified_key} = {modified_value}\n\t'
        self.logger.info(f'exiting modify_for_function_variables with '
                         f'output {result}')
        return result

    def validate_declarative_json(self, pipeline_info):
        """
        validates the mandatory fields in the provided declarative json
        Args:
            pipeline_info: contents of the json file

        Returns: Raises exception in case of inconsistency

        """
        self.logger.info('entering validate_declarative_json method')
        if not pipeline_info:
            self.logger.error('Declarative JSON empty.')
            raise IncorrectDeclarativeJSONDefinitionException(
                'Declarative JSON empty.')
        pipeline_fields = ['name', 'description', 'pvc_name', 'components',
                           'main_func_params']
        for field in pipeline_fields:
            if field not in pipeline_info.keys():
                self.logger.error(f'Field "{field}" not present in '
                                  f'declarative JSON')
                raise IncorrectDeclarativeJSONDefinitionException(
                    f'Field "{field}" not present in declarative JSON')

    def validate_component_keys(self, component_keys):
        """
        Validates if the component has all default keys present
        Args:
            component_keys: keys present in the component

        Returns: nothing

        """
        default_keys = ['name', 'xpresso_reference', 'description', 'inputs',
                        'input_values', 'implementation']
        for key in default_keys:
            if key not in component_keys:
                self.logger.error(f'Key "{key}" is missing from one or more '
                                  f'components in pipeline JSON')
                raise ComponentsSpecifiedIncorrectlyException(
                    f'Key "{key}" is missing from one or more components '
                    f'in pipeline JSON')

    def generate_pipeline_file(self, pipeline_info, component_images,
                               pipeline_deploy_id):
        """
        generates a python dsl pipeline file using the provided declarative
        json, executes it and uploads the pipeline to kubeflow.
        Args:
            component_images: dict of pipeline components and their
            corresponding docker images
            pipeline_info: declarative json file containing info
            about pipeline
            pipeline_deploy_id : deploy version id of pipeline fetched from
                                database
        Returns: ambassador port to view the pipeline on dashboard

        """
        self.logger.info('entering generate_python_file method')
        self.logger.debug('reading declarative json')

        # check for mandatory fields
        self.validate_declarative_json(pipeline_info)

        # generate code to load pipeline component objects
        components_info = self.generate_pipeline_component_objects(
            pipeline_info)

        # populate the pipeline name and description
        self.populate_name_and_description(pipeline_info)

        # populate main function's parameters
        self.populate_main_func_parameters(pipeline_info)

        # populate main function's variables, if any
        self.populate_main_func_variables(pipeline_info)

        # populate container op, if present
        self.populate_container_op(pipeline_info)

        # generate and populate component definitions with inputs
        self.populate_component_definitions(pipeline_info, components_info)

        # update pipeline yaml location
        pipeline_yaml_location = self.update_pipeline_yaml_location(
            pipeline_deploy_id, pipeline_info)

        # finally, populate and generate the python file
        self.generate_pipeline_python_file(pipeline_deploy_id, pipeline_info)

        # create yaml file for the generated python file to read components from
        self.create_pipeline_yaml(component_images, pipeline_info,
                                  pipeline_yaml_location)

        # run the generated python file to generate the zip file
        self.logger.debug('running generated python file')
        pipeline_file = f'{self.declarative_pipeline_folder}' \
            f'/{pipeline_info["name"]}--declarative_pipeline' \
            f'_{pipeline_deploy_id}.py'
        run_pipeline_python = f'python {pipeline_file}'
        status = self.executor.execute(run_pipeline_python)
        if status:
            raise IncorrectDeclarativeJSONDefinitionException(
                "Failed to run pipeline dsl file. "
                "Please re-check the declarative JSON file.")
        pipeline_zip = f'{pipeline_file}.zip'
        return pipeline_zip

    def create_pipeline_yaml(self, component_images, pipeline_info,
                             pipeline_yaml_location):
        """
        creates yaml file for dsl code to read components from
        Args:
            component_images: dict of pipeline components and their
            corresponding docker images
            pipeline_info: pipeline info from declarative json
            pipeline_yaml_location: location where the file is to be generated

        Returns: nothing

        """
        self.logger.debug('creating yaml for generated python file')
        temp_pipeline = deepcopy(pipeline_info)
        modified_components = temp_pipeline['components']
        for component in modified_components:
            component['implementation']['container']['image'] \
                = component_images[component['xpresso_reference']]
            del component['xpresso_reference']
            del component['input_values']
        data_to_insert = {"components": modified_components}
        with open(pipeline_yaml_location, 'w+') as f:
            f.write(yaml.dump(data_to_insert))

    def generate_pipeline_python_file(self, pipeline_deploy_id, pipeline_info):
        """
        generates pipeline python file
        Args:
            pipeline_deploy_id: deploy version id of pipeline fetched from
                                database
            pipeline_info: pipeline info from declarative json

        Returns: nothing

        """
        self.logger.debug('generating python file')
        with open(f'{self.declarative_pipeline_folder}/{pipeline_info["name"]}'
                  f'--declarative_pipeline_{pipeline_deploy_id}.py', 'w+') as f:
            f.write(self.content)

    def update_pipeline_yaml_location(self, pipeline_deploy_id, pipeline_info):
        """
        updates location where pipeline yaml will be generated
        Args:
            pipeline_deploy_id: deploy version id of pipeline fetched from
                                database
            pipeline_info: pipeline info from declarative json

        Returns: yaml location

        """
        pipeline_yaml_location = f"{self.declarative_pipeline_folder}" \
            f"/{pipeline_info['name']}--pipeline_components_file_" \
            f"{pipeline_deploy_id}.yaml"
        self.content = self.content.replace('%pipeline_yaml_location%',
                                            f"'{pipeline_yaml_location}'")
        return pipeline_yaml_location

    def populate_container_op(self, pipeline_info):
        """
        populates container op
        Args:
            pipeline_info: pipeline info from declarative json

        Returns: nothing

        """
        if 'container_op' not in pipeline_info.keys():
            self.logger.debug('container op not present')
            self.content = self.content.replace('%container_op%', '')
        else:
            self.logger.debug('populating container op')
            checkout = f"""\t{str(pipeline_info['container_op'][
                                      '$$name$$'])} = dsl.ContainerOp({self.modify_for_function_parameters(
                pipeline_info['container_op'])})"""
            if 'checkout' in pipeline_info['after_dependencies'].keys():
                checkout = checkout + f"""\n\n\tcheckout.after({
                pipeline_info['after_dependencies']['checkout']})"""
            self.reference_array.append('checkout')
            self.content = self.content.replace('%container_op%', checkout)

    def populate_main_func_variables(self, pipeline_info):
        """
        populates main function variables
        Args:
            pipeline_info: pipeline info from declarative json

        Returns: nothing

        """
        if 'main_func_variables' in pipeline_info.keys():
            self.logger.debug("populating main function's variables")
            main_variables = "\t" + self.modify_for_function_variables(
                pipeline_info['main_func_variables'])
            self.content = self.content.replace('%main_function_variables%',
                                                main_variables)
        else:
            self.logger.debug('No variables found for main function')
            self.content = self.content.replace('%main_function_variables%', '')

    def generate_pipeline_component_objects(self, pipeline_info):
        """
        generates code to load pipeline component objects
        Args:
            pipeline_info: pipeline info from declarative json

        Returns: components info

        """
        self.logger.info('generating code to load pipeline component objects')
        pipeline_comps = """"""
        components_info = pipeline_info['components']
        self.reference_array.extend([comp['name'] for comp in components_info])
        for index, component in enumerate(components_info):
            self.validate_component_keys(component.keys())
            pipeline_comps = pipeline_comps + f"{component['name']}_ = " \
                f"components.load_component_from_text(str(" \
                f"component_info[{index}]))\n"
        self.content = self.content.replace('%load_components%', pipeline_comps)
        return components_info

    def populate_name_and_description(self, pipeline_info):
        """
        populates the pipeline name and description
        Args:
            pipeline_info: pipeline info from declarative json

        Returns: nothing

        """
        self.logger.debug('populating the pipeline name and description')
        self.content = self.content.replace("%pipeline_name%",
                                            f"'{pipeline_info['name']}'")
        self.content = self.content.replace('%pipeline_description%',
                                            f"'{pipeline_info['description']}'")

    def populate_main_func_parameters(self, pipeline_info):
        """
        populates main function parameters
        Args:
            pipeline_info: pipeline info from declarative json

        Returns: nothing

        """
        self.logger.debug("populate main function's parameters")
        main_params = self.modify_for_function_parameters(
            pipeline_info['main_func_params'])
        self.content = self.content.replace('%main_function_params%',
                                            main_params)

    def populate_component_definitions(self, pipeline_info, components_info):
        """
        populates component definitions
        Args:
            pipeline_info: pipeline info from declarative json
            components_info: components info in declarative json

        Returns: nothing

        """
        self.logger.debug('populating component definitions with inputs')
        component_definitions = """"""
        for index, component in enumerate(components_info):
            if index == 0:
                add_pvc = \
                    f"add_volume(k8s_client.V1Volume(name='pipeline-nfs', " \
                    f"persistent_volume_claim=k8s_client." \
                    f"V1PersistentVolumeClaimVolumeSource(claim_name=" \
                    f"'{pipeline_info['pvc_name']}'))).add_volume_mount(" \
                    f"k8s_client.V1VolumeMount(" \
                    f"mount_path='/data', name='pipeline-nfs'))"
            else:
                add_pvc = "add_volume_mount(k8s_client.V1VolumeMount(" \
                          "mount_path='/data', name='pipeline-nfs'))"
            component_definitions = \
                component_definitions + \
                f"\t{component['name']} = {component['name']}_(" \
                f"{self.modify_for_function_parameters(component['input_values'])}).{add_pvc}\n\n"

            if 'after_dependencies' in pipeline_info.keys():
                if component['name'] in pipeline_info['after_dependencies'].keys():
                    component_definitions = \
                        component_definitions + \
                        f"\t{component['name']}.after({pipeline_info['after_dependencies'][component['name']]})\n\n"
        self.content = self.content.replace('%component_definitions%',
                                            component_definitions)
