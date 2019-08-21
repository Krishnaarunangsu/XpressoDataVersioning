import json

def process_json(json):
    """

    :param json:
    :return:
    """
    print(f'JSON Coming:{json}')
    if 'info' not in json:
        print('Krishna')

if __name__ == "__main__":
    with open('x.json', 'rb') as f:
        process_json(f)
