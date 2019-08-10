from xpresso.ai.admin.controller.utils import error_codes
import simplejson as json
from pprint import pprint

error_response_path = '/opt/xpresso.ai/config/error_response.json'
success_response_path = '/opt/xpresso.ai/config/success_response.json'

try:
    with open(error_response_path, 'r', encoding='utf-8') as jsonfile:
        error_response = json.load(jsonfile)
except FileNotFoundError:
    error_response = {}

try:
    with open(success_response_path, 'r', encoding='utf-8') as jsonfile:
        success_response = json.load(jsonfile)
except FileNotFoundError:
    success_response = {}


def response(responseobject, requesttype, resultflag, successtr):
    if responseobject['outcome'] == 'success':
        results = responseobject['results']
        if resultflag:
            pprint(results)
            return
        try:
            print(f"### {success_response[requesttype][successtr]} ###")
        except KeyError:
            print("### OK ###")
        return
    elif responseobject['outcome'] == 'failure':
        code = str(responseobject['error_code'])
        try:
            output = '=== ' + error_response[requesttype][code] + ' ==='
        except KeyError:
            output = "=== Operation Failed ==="
        print(output)
        return
    else:
        return
