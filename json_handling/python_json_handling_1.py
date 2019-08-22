import json

def process_json(json):
    """

    :param json:
    :return:
    """
    print(f'JSON Coming:{json}')
    if 'info' not in json:
        print('Krishna')
    else:
        print('info is present')

if __name__ == "__main__":
    with open('../resources/create_repo.json', 'rb') as f:
        data = json.load(f)
        process_json(data)
