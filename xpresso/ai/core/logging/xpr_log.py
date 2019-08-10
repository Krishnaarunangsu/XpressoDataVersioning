""" Log management for Xpresso Project"""
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    InvalidConfigException

__all__ = ['XprLogger']
__author__ = 'Srijan Sharma'

import logging
import datetime
import os
import json
from logstash_async.handler import AsynchronousLogstashHandler
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.utils.singleton import Singleton


class XprLogger(logging.Logger, metaclass=Singleton):
    """
    Creates a logger object to put the logs into a file and index them into elastic search.
    """

    LOGGING_SECTION = "log_handler"
    LOGSTASH_HOST = "host"
    LOGSTASH_PORT = "port"
    LOGSTASH_CACHE_BOOL = "cache_in_file"
    LOGGING_LOGSTASH_BOOL = "log_to_elk"
    LOGGING_FILE_BOOL = "log_to_file"
    LOGS_FOLDER_PATH = "logs_folder_path"
    FORMATTER = "formatter"
    PROJECT_NAME = "Project Name"
    FIND_CONFIG_RECURSIVE = "find_config_recursive"

    def __init__(self, level=logging.DEBUG):

        self.xpr_config = XprConfigParser(
            config_file_path=XprConfigParser.DEFAULT_CONFIG_PATH_XPR_LOG)
        if self.xpr_config[self.LOGGING_SECTION][self.FIND_CONFIG_RECURSIVE]:
            self.xpr_config = self.load_config("xpr")

        self.name = self.xpr_config[self.PROJECT_NAME]
        super(XprLogger, self).__init__(self.name)

        self.setLevel(level)

        logger_formatter = XprCustomFormatter(
            self.xpr_config[self.LOGGING_SECTION][self.FORMATTER])
        logstash_formatter = XprLogstashCustomFormatter(
            self.xpr_config[self.LOGGING_SECTION][self.FORMATTER])

        log_folder = os.path.expanduser(
            self.xpr_config[self.LOGGING_SECTION][self.LOGS_FOLDER_PATH])
        if not os.path.exists(log_folder):
            try:
                os.makedirs(log_folder, 0o755)
            except IOError as err:
                print(
                    "Permission Denied to create logs folder at the specidied directory. \n{}".format(
                        str(err)))

        # Adding file handler for levels below warning
        try:
            if self.xpr_config[self.LOGGING_SECTION][self.LOGGING_FILE_BOOL]:
                try:
                    wfh = logging.FileHandler(os.path.join(
                        log_folder,
                        '.'.join((self.xpr_config[self.PROJECT_NAME], "log"))), 'w')
                except IOError as err:
                    print("Permission denied to create log files. "
                          "Saving log files in base directory . \n{}".format(
                        str(err)))
                    wfh = logging.FileHandler(
                        os.path.join(os.path.expanduser("~"),
                                     '.'.join((self.xpr_config[
                                                   self.PROJECT_NAME], "log"))),
                        'w')
                wfh.setFormatter(logger_formatter)
                wfh.setLevel(logging.DEBUG)
                self.addHandler(wfh)
        except Exception as err:
            print("Unable to add file handler to logger. \n{}".format(str(err)))
            raise err

        # Adding file handler for levels more critical than warning
        try:
            if self.xpr_config[self.LOGGING_SECTION][self.LOGGING_FILE_BOOL]:
                try:
                    efh = logging.FileHandler(os.path.join(
                        log_folder,
                        '.'.join((self.xpr_config[self.PROJECT_NAME], "err"))), 'w')
                except IOError as err:
                    print("Permission denied to create log files. "
                          "Saving log files in base directory . \n{}".format(
                        str(err)))
                    efh = logging.FileHandler(
                        os.path.join(os.path.expanduser("~"),
                                     '.'.join((self.xpr_config[
                                                   self.PROJECT_NAME], "err"))),
                        'w')
                efh.setFormatter(logger_formatter)
                efh.setLevel(logging.ERROR)
                self.addHandler(efh)
        except Exception as err:
            print(
                "Unable to add file handler to logger . \n{}".format(str(err)))
            raise err

        # Adding logstash logging handler
        try:
            if self.xpr_config[self.LOGGING_SECTION][
                self.LOGGING_LOGSTASH_BOOL]:
                cache_filename = ""
                if self.xpr_config[self.LOGGING_SECTION][
                    self.LOGSTASH_CACHE_BOOL]:
                    cache_filename = os.path.join(
                        log_folder, "cache.persistence")

                lh = AsynchronousLogstashHandler(
                    host=self.xpr_config[self.LOGGING_SECTION][
                        self.LOGSTASH_HOST],
                    port=self.xpr_config[self.LOGGING_SECTION][
                        self.LOGSTASH_PORT],
                    database_path=cache_filename)
                lh.setFormatter(logstash_formatter)
                self.addHandler(lh)
        except Exception as err:
            print("Unable to add logstash handler to logger. \n{}".format(
                str(err)))
            raise err

    def filter(self, record):
        record.projectname = self.xpr_config[self.PROJECT_NAME]
        return True

    def find_config(self, config_log_filename):
        """
        Iterates over the whole directory structure to find the path of the config file
        Args:
            config_log_filename(str): name of the config file to be looked for

        Returns:
            str : Absolute Filepath of the configuration file

        """
        filepath = ""
        for dirpath, subdirs, files in os.walk(os.getcwd()):
            for x in files:
                if x.endswith(config_log_filename):
                    filepath = os.path.join(dirpath, x)
                    return filepath
        if not filepath:
            raise FileNotFoundError

    def load_config(self, config_log_type) -> XprConfigParser:
        """
        Args:
            config_log_type(str): Type of the config file i.e xpr or setup

        Returns:
            str : configuration filename based on the type provided

        """

        if config_log_type == "setup":
            config_log_filename = "setup_log.json"
        elif config_log_type == "xpr":
            config_log_filename = "xpr_log.json"
        else:
            raise ValueError("Invalid parameter passed to load_config")

        config = None
        try:
            config = XprConfigParser(
                os.path.join(self.find_config(config_log_filename)))
        except FileNotFoundError as err:
            # This is intended
            pass
        except InvalidConfigException as err:
            print(
                "Invalid config Found. Loading from the config from default path. \n{}".format(
                    str(err)))
        finally:
            if config is None:
                try:
                    config = XprConfigParser(
                        XprConfigParser.DEFAULT_CONFIG_PATH_XPR_LOG)
                except FileNotFoundError as err:
                    print(
                        "Unable to file the config file in base directory. Loading from the config "
                        "from default path. \n{}".format(str(err)))
                    raise err
                except InvalidConfigException as err:
                    print("Invalid config Found. \n{}".format(str(err)))
                    raise err
        return config


class XprCustomFormatter(logging.Formatter):
    """
    Takes the record and formats the log
    in a very specific way
    """

    def __init__(self, formatter):
        super().__init__()
        self.formatter = formatter

    def format(self, record):
        log_values = [
            datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "%s=%s" % ("projectname", getattr(record, "projectname"))]
        for key in self.formatter:
            if self.formatter[key]:

                try:
                    if key == "exc_info":
                        log_values.append("%s=%s" % (
                            key, self.formatException(getattr(record, key))))
                        continue
                except Exception as err:
                    continue
                log_values.append("%s=%s" % (key, getattr(record, key)))
        return " ".join(log_values)


class XprLogstashCustomFormatter(logging.Formatter):
    """
    Takes the record and formats the log
    in a very specific way
    """

    def __init__(self, formatter):
        self.formatter = formatter

    def format(self, record):
        log_values = dict()
        log_values["timestamp"] = datetime.datetime.utcnow().strftime(
            '%Y-%m-%dT%H:%M:%S.%fZ')
        log_values["projectname"] = getattr(record, "projectname")
        for key in self.formatter:
            if self.formatter[key]:
                log_values[str(key)] = str(getattr(record, key))
        return json.dumps(log_values)


if __name__ == '__main__':
    main_logger = XprLogger()
    main_logger.info("Test message")
