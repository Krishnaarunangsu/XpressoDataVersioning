""" Utility methods for linux VM"""

import os
import subprocess
import shutil


def check_root():
    """ Check if current user has root privileges """
    if os.geteuid() == 0:
        return True
    return False


def get_ip_address():
    hostname_out = subprocess.check_output(['hostname', '-I'])
    ip_address_str = hostname_out.decode('ascii').split()[0]
    return ip_address_str


def create_directory(path, permission):
    try:
        os.makedirs(path, mode=permission, exist_ok=True)
    except OSError:
        return False
    return True


def write_to_file(content, path_to_file, mode):
    """
    :param content: what to write inside file
    :param path_to_file: path to file
    :param mode: write (w+), append (a), etc..
    :return:
    """
    file = open(path_to_file, mode)
    file.write("\n")
    file.write(content)
    file.close()


def check_if_a_command_installed(command_name):
    """
    Check if a command is installed on a VM
    Args:
        command_name(str): Name of the command
    Returns
        bool: True if present, False otherwise
    """
    return shutil.which(command_name) is not None
