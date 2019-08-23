# A python program to create user-defined exception
# class MyError is derived from super class Exception


class InvalidDataTypeException(Exception):

    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


class SerializationFailedException(Exception):

    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


class DeserializationFailedException(Exception):

    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


class PrestoConnectionException(Exception):

    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)



class PachydermOperationException(Exception):
    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


class RepoNotProvidedException(Exception):
    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


class PachydermFieldsNameException(Exception):
    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


class BranchInfoException(Exception):
    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


class DatasetInfoException(Exception):
    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


class DatasetPathException(Exception):
    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


class LocalFilePathException(Exception):
    # Constructor or Initializer
    def __init__(self, value):
        self.value = value

        # __str__ is to print() the value

    def __str__(self):
        return repr(self.value)


