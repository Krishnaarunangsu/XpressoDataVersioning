""" Parses configuration file to create an internal dictionary file"""

__all__ = ['XprConfigParser']
__author__ = 'Naveen Sinha'

import json
from json import JSONDecodeError

from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    InvalidConfigException


class XprConfigParser:
    """ Parses properties file and stores them in the internal dictionary file.
    It expects the location of config file as an input. By Default, it searches
    config in config/common.json.
    """

    DEFAULT_CONFIG_PATH = "/opt/xpresso.ai/config/common.json"
    DEFAULT_CONFIG_PATH_SETUP_LOG = "/opt/xpresso.ai/config/setup_docker.json"
    DEFAULT_CONFIG_PATH_XPR_LOG = "/opt/xpresso.ai/config/xpr_log.json"

    def __init__(self, config_file_path=DEFAULT_CONFIG_PATH):
        self.config_json = None
        with open(config_file_path, 'r', encoding='utf-8') as config_fs:
            try:
                self.config_json = json.load(config_fs)
            except (JSONDecodeError, TypeError) as e:
                raise InvalidConfigException("{} is invalid json in config. {}"
                                             .format(config_file_path,
                                                     str(e())))

        if not self.config_json:
            raise InvalidConfigException("{} is invalid config"
                                         .format(config_file_path))

    def __getitem__(self, key):
        """
        Overriding get method to support direct return from self.config_json
        """
        return self.config_json[key]
