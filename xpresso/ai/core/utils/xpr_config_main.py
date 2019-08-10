import json
from json import JSONDecodeError

from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    InvalidConfigException

# read file

DEFAULT_CONFIG_PATH = "common.json"


class XprConfigParserMain:
    """

    """

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
"""  
with open('common.json', 'r') as myfile:
    data=myfile.read()

# parse file
obj = json.loads(data)
"""

if __name__ == "__main__":
    p =XprConfigParserMain(DEFAULT_CONFIG_PATH)



