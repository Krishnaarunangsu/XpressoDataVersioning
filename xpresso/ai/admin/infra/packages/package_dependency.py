""" Package Dependency MOdule
"""
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    PackageFailedException

__all__ = ["PackageDependency"]
__author__ = "Srijan Sharma"

import json
import os
import networkx as nx
import matplotlib.pyplot as plt

from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.logging.xpr_log import XprLogger


class PackageDependency:
    """
    Created a  directed acyclic package dependency graph
    using a given dependency json.
    """

    NONE_PACKAGE = "None"
    DEPENDENCY_SECTION = "pkg_dependency"
    DEPENDENCY_CONFIG_FILE = "dependency_config_file"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH):
        super().__init__()
        self.config = XprConfigParser(config_path)["packages_setup"]
        self.logger = XprLogger()
        dependency_config_file = self.config[self.DEPENDENCY_SECTION][
            self.DEPENDENCY_CONFIG_FILE]

        if not os.path.exists(dependency_config_file):
            self.logger.error(("Unable to find the dependency js"
                               "file at the mentioned path"))
            raise PackageFailedException("Invalid dependency  config file")

        try:
            with open(dependency_config_file) as config_fs:
                dependency_config = json.load(config_fs)
        except EnvironmentError as err:
            self.logger.fatal(err)
            raise PackageFailedException("Invalid config file")

        self.graph = nx.DiGraph()
        edges = list()

        for key in dependency_config:
            for value in dependency_config[key]:
                edges.append((key, value))

        self.graph.add_edges_from(edges)
        if not nx.is_directed_acyclic_graph(self.graph):
            self.logger.fatal(("Unable to handle dependencies due to cyclic "
                               "loop"))
            self.graph = None
            raise PackageFailedException("Cyclic Dependency Found")

    def visualize_dependency_graph(self):
        """
        Created a plot for the directed dependency graph
        """
        if self.graph is None:
            self.logger.error("Graph value none cannot be plotted")
            return

        nx.draw(self.graph, cmap=plt.get_cmap('jet'), with_labels=True)
        plt.show()

    def check_if_supported(self, package_name: str):
        """
        Args:
            package_name(str)

        :return:
            bool: Return True if supported. False, otherwise
        """
        return bool(self.graph.has_node(package_name))

    def list_all(self):
        """
        Extracts the value of all nodes(packages) present in graph

        Returns:
            list: Array consisting of all node(packages) value
        """
        if self.graph is None:
            self.logger.error("Graph value none cannot be iterated")
            return list()

        nodes = list()
        for node in self.graph.nodes():
            if node == self.NONE_PACKAGE:
                continue
            nodes.append(node)
        return nodes

    def get_dependency(self, package_name: str) -> list:
        """
        List of dependencies

        Args:
            package_name(str): Name of the package

        Returns:
            list: List of dependencies required for the package_name
                  installation
        """

        if not self.check_if_supported(package_name=package_name):
            self.logger.error("{} package not present in config"
                              .format(package_name))
            return list()

        self.logger.info(("Running Topological sorting on "
                          "Package Dependency Graph"))

        try:
            topological_sort_list = list(reversed(list(
                nx.topological_sort(self.graph))))
        except nx.NetworkXError as error:
            self.logger.error(error)
            raise PackageFailedException("Topological sort is defined for "
                                         "directed graphs only")
        except nx.NetworkXUnfeasible as error:
            self.logger.error(error)
            raise PackageFailedException("Not a directed acyclic graph (DAG) "
                                         "and hence no topological sort exists")

        descendants = nx.descendants(self.graph, package_name)
        dependent_packages = []
        for pkg in topological_sort_list:
            if pkg in descendants and pkg != self.NONE_PACKAGE:
                dependent_packages.append(pkg)
        if package_name != self.NONE_PACKAGE:
            dependent_packages.append(package_name)

        return dependent_packages


if __name__ == "__main__":
    pkg_dep = PackageDependency()
    pkg_dep.visualize_dependency_graph()
    print(pkg_dep.list_all())
    print(pkg_dep.get_dependency("PythonPackage"))
