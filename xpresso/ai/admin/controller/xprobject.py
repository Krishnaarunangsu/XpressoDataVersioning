from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.core.logging.xpr_log import XprLogger


class XprObject(object):
    """
    This class serves as the superclass of all objects managed by the controller
    e.g., User, Node, Cluster, etc.
    """
    def __init__(self, objjson=None):
        self.logger = XprLogger()
        self.logger.debug("Inside XprObject constructor")
        self.data = objjson
        self.logger.debug("Done")

    def set(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data[key]

    def validate_mandatory_fields(self):
        self.logger.debug("Validating mandatory fields")
        """
        checks if the mandatory fields for an object have been specified
        """
        for f in self.mandatory_fields:
            self.logger.debug("Validating field {}".format(f))
            if f not in self.data:
                raise MissingFieldException("Field '{}' missing in "
                                            "input".format(f))
            elif not len(self.data[f]):
                raise BlankFieldException("Field '{}' blank in input".format(f))

    def validate_field_values(self):
        """
        checks if the value of the specified field is valid
        :param objjson: JSON object
        :param field: field to bee checked
        :param valid_values: list of valid values for string
        """
        for field in self.valid_values:
            if field in self.data:
                if self.data[field] not in self.valid_values[field]:
                    raise InvalidValueException("Value {} invalid for field {} in input".format(self.data[field], field))

    def validate_modifiable_fields(self):
        self.logger.debug("Validating modifiable fields")
        """
        checks if the mandatory fields for an object have been specified
        """
        for f in self.unmodifiable_fields:
            self.logger.debug("Validating field {}".format(f))
            if f in self.data:
                raise IllegalModificationException("Field {} cannot be modified".format(f))

    def filter_display_fields(self):

        filtered_data = {}
        for field in self.display_fields:
            if field in self.data:
                filtered_data[field] = self.data[field]
        self.data = filtered_data
