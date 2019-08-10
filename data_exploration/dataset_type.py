from enum import Enum

DECIMAL_PRECISION = 2


class DatasetType(Enum):
    """ Enum class for structured, semi structured
    and unstructured data type"""

    STRUCTURED = "structured"
    SEMI_STRUCTURED = "semi-structured"
    UNSTRUCTURED = "unstructured"

    def __str__(self):
        return self.value
