import json
from datetime import date
import pprint


# Owner object
class Owner(object):
    """
    Owner class
    """
    def __init__(self):
        self.uid = 'ASahu2'

# Components class
class Component(object):
    """
    Component class
    """
    def __init__(self):
        self.name = ''
        self.type = ''
        self.flavor = ''

# Instances of Componenr Object to go in 'components' list
component_1 = Component()
component_1.name = "service-reader"
component_1.flavor = 'python'
component_1.type = 'service'

component_2 = Component()
component_2.name = "database-service"
component_2.flavor = 'sql'
component_2.type = 'database'


# Create Project
class CreateProject(object):
    """
    CreateProject
    """
    def __init__(self):
        self.name = "pipeline_project"
        self.description = "Project for Pipeline"
        self.owner = Owner().__dict__
        self.developers = ["ASahu4", "asarkar1"]
        self.components = [component_1.__dict__ ,component_2.__dict__]

# function not method
def serialize(obj):
    if isinstance(obj, date):
        serial = obj.isoformat()
        return serial

    return obj.__dict__

# CreateProject Object
create_project = CreateProject()
create_project_dict :dict = serialize(create_project)
create_project_dict_json = json.dumps(create_project_dict, indent=2, sort_keys=True)
print(f'JSON:\n{create_project_dict_json}')
# pp = pprint.PrettyPrinter(indent=4)
# print(f'Dictionary:"\n{create_project_dict}')
# pp.pprint(serialize(create_project))
#python dict to json


#pp.pprint(create_project_dict_json)



# https://docs.python.org/3/library/pprint.html
# https://appdividend.com/2019/04/15/how-to-convert-python-dictionary-to-json-tutorial-with-example/
# https://stackoverflow.com/questions/12943819/how-to-prettyprint-a-json-file
# https://codeblogmoney.com/json-pretty-print-using-python/
