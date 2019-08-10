""" Installs packages within a machine"""

import argparse
import ast

from xpresso.ai.admin.infra.packages.package_manager import ExecutionType
from xpresso.ai.admin.infra.packages.package_manager import PackageManager
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    PackageFailedException

CONFIG_SECTION = "xpr_pkg"
CONFIG_MANIFEST_KEY = "manifest_path"


def main():
    parser = parse_arguments()
    args = parser.parse_args()

    package_manager = PackageManager(args.conf)

    if args.type and args.parameters:
        # Check if args.parameters is a valid dict
        try:
            parameters = ast.literal_eval(args.parameters)
        except (ValueError, TypeError):
            raise PackageFailedException("Parameter provided is not valid dict")
        package_manager.run(package_to_install=str(args.package),
                            execution_type=args.type,
                            parameters=parameters)
    elif args.type:
        package_manager.run(package_to_install=str(args.package),
                            execution_type=args.type)
    elif args.list:
        package_list = package_manager.list()
        print("Supported Package List : {}".format(package_list))
    else:
        parser.print_usage()


def parse_arguments():
    """ Reads commandline argument to identify which clients group to
    install
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--conf",
                        required=True,
                        help="Root configuration file")
    parser.add_argument("--package",
                        type=str,
                        required=False,
                        help="Package group name to install. Must "
                             "match the name from manifest file")
    parser.add_argument("--type",
                        type=ExecutionType,
                        choices=list(ExecutionType),
                        required=False,
                        help="Package group name to install. Must "
                             "match the name from manifest file")
    parser.add_argument("--list",
                        action='store_true',
                        required=False,
                        default=False,
                        help="List supported package")
    parser.add_argument("--parameters",
                        type=str,
                        required=False,
                        help="Dictionary of Additonal parameters")
    return parser


if __name__ == "__main__":
    main()
