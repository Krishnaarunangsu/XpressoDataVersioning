import json


# In Python, JSON exists as a string. For example:
json_data: str = '{"firstName" : "Arunangsu","lastName" : "Sahu"}'
print(json_data)
print(type(json_data))
print('********************************************************')
# Python JSON to dict
json_data_dict = json.loads(json_data)
print(json_data_dict)
print(type(json_data_dict))
print(json_data_dict['firstName'])
