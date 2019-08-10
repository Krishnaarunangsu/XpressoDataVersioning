"""Responsible for installing the os packages in a machine. """

__all__ = ['PackageManager', 'ExecutionType']
__author__ = 'Naveen Sinha'

import sys
import inspect
import pkgutil
import platform
from enum import Enum

from xpresso.ai.admin.infra.packages.package_dependency import PackageDependency
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.infra import packages
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    PackageFailedException
from xpresso.ai.core.logging.xpr_log import XprLogger


class ExecutionType(Enum):
    """
    Enum class to standardize all the execution type
    """
    INSTALL = "install"
    UNINSTALL = "uninstall"
    START = "start"
    STOP = "stop"
    STATUS = "status"

    def __str__(self):
        return self.value


class PackageManager:
    """Manages the request for package installation and setup
    """

    MANIFEST_SCRIPT_KEY = "script_path"
    MANIFEST_MULTI_ARG_KEY = "script_multi_arguments"
    MANIFEST_DEPENDENCY_KEY = "dependency_packages"
    MANIFEST_SSH_CONFIG = "ssh_config"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH):
        """
        1. Generate metadata of existing VM
        2. Reads the arguments and initiates the instance variable
        """

        self.logger = XprLogger()

        self.python_version = sys.version.split('\n')
        self.system = platform.system()
        self.machine = platform.machine()
        self.platform = platform.platform()
        self.uname = platform.uname()
        self.version = platform.version()
        self.arch = platform.architecture()
        self.config_path = config_path
        self.package_dependency = PackageDependency(config_path=config_path)

    def list(self):
        """ List all supported package group name

        Returns:
            list: list of available packages
        """
        return self.package_dependency.list_all()

    def run(self, package_to_install: str, execution_type: ExecutionType,
            parameters: dict = None):
        """
        Perform provided execution type on the given package name

        Args:
            parameters(dict): Additional parameters
            package_to_install(str): name of the package to install. Must match
                                     the supported package names
            execution_type(ExecutionType): Type of execution
        """
        if not self.package_dependency.check_if_supported(package_to_install):
            self.logger.error("Unsupported Package Name : {}"
                              .format(package_to_install))
            return False

        dependency_list = self.package_dependency.get_dependency(
            package_to_install)
        self.logger.info(dependency_list)
        response = self.execute_recursive(dependency_list, 0, execution_type,
                                          parameters=parameters)
        if not response:
            raise PackageFailedException("Package installation failed!!")
        self.logger.info("{} installed successfully".format(package_to_install))
        return True

    def execute_recursive(self, dependency_list: list, current_index: int,
                          execution_type: ExecutionType,
                          parameters: dict = None):
        """
        Execute  recursively. If something failed then rollback
        """
        if current_index >= len(dependency_list):
            return True

        package_string = dependency_list[current_index]
        self.logger.info(package_string)
        try:
            self.execute(
                package_class=self.package_str_to_class(
                    package_string,
                    packages),
                execution_type=execution_type,
                parameters=parameters)
            current_index += 1
            return self.execute_recursive(dependency_list, current_index,
                                          execution_type, parameters=parameters)
        except PackageFailedException:
            self.logger.error("Failed to execute package {}"
                              .format(str(package_string)))
        return False

    def execute(self,
                package_class,
                execution_type: ExecutionType,
                parameters: dict = None):
        """
        Perform provided execution type on the given package name

        Args:
            parameters: Additional parameter required for installation
            package_class: name of the package to install.
                           Must match the supported package names.
            execution_type(ExecutionType): Type of execution
        Returns:
            bool: True,if the execution is successful
        """
        if package_class is None:
            self.logger.info("{} Not Found in the hirarchy".
                             format(package_class))
            return False
        self.logger.info(f"Running package {package_class} with parameters"
                         f"{parameters}")
        package_obj = package_class(config_path=self.config_path)
        if (execution_type == ExecutionType.INSTALL and
                package_obj.status(parameters=parameters)):
            return True
        elif execution_type == ExecutionType.INSTALL:
            return package_obj.install(parameters=parameters)
        elif execution_type == ExecutionType.UNINSTALL:
            return package_obj.uninstall(parameters=parameters)
        elif execution_type == ExecutionType.STATUS:
            return package_obj.status(parameters=parameters)
        elif execution_type == ExecutionType.START:
            return package_obj.start(parameters=parameters)
        elif execution_type == ExecutionType.STOP:
            return package_obj.stop(parameters=parameters)

        self.logger.error(str(package_obj) + " Not defined")
        return False

    def package_str_to_class(self, target_class_name: str, package_name):
        """ Converts class name into python class object. It looks for all
        classes in a package and matches the name to the class object

        Args:
            package_name(package): Find the target class name within this
                                   package name
            target_class_name(str): exact name of the class. It should match the
                                    class name as well

        Returns:
            Object: returns python class object, None otherwise
        """

        for _, modname, is_pkg in \
            pkgutil.iter_modules(package_name.__path__,
                                 package_name.__name__ + "."):
            imported_module = __import__(modname, fromlist=["dummy"])
            matched_class_name = None
            if is_pkg:
                matched_class_name = self.package_str_to_class(
                    target_class_name,
                    imported_module)
            if matched_class_name:
                return matched_class_name
            for name, obj in inspect.getmembers(imported_module):
                if inspect.isclass(obj) and name == target_class_name:
                    return obj
        return None
