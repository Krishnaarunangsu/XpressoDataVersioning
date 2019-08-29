import json
from datetime import date

# Python save objects as json file
class Person(object):
    def __init__(self):
        self.name = 'John'
        self.age = 25
        self.id = 1



def serialize(obj):
    if isinstance(obj, date):
        serial = obj.isoformat()
        return serial

    return obj.__dict__


person = Person()
print(serialize(person))


# https://blog.softhints.com/python-convert-object-to-json-3-examples/
