"""
This file contains generic utility functions
"""


def get_version():
    """
    Fetches client and server versions and prints out in the stdout

    Returns:
        str: version string of the project
    """
    try:
        version_file_name = 'VERSION'
        version_fs = open(version_file_name)
        version = version_fs.read().strip()
        version_fs.close()
    except FileNotFoundError:
        # Using default version
        version = '-1'
    return version
