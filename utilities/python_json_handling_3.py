import json

class JSONHandler:
    """
    class for JSON Handling
    """
    def __init__(self):
        """
        Initialization
        """

    def process_json(self,json_data):
        """

        :return:
        """
        print(json_data)
        print('who')
        if 'name' not in json_data:
            print('Something wrong')
        else:
            print('Name is present')


if __name__ == "__main__":
    json_handler = JSONHandler()
    with open('../resources/person.json') as json_file:
        print('coming here')
        #json_handler.process_json(json_file)
        json_data = json.load(json_file)
        json_handler.process_json(json_data)
