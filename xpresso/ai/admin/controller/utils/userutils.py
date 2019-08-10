from xpresso.ai.admin.controller.utils import error_codes


def userinfocheck(userjson):
    """
    userinfocheck checks if the userdata provided is sufficient enough
    """
    # These are mandatory fields that needs to be provided in userjson
    requiredinfo = [
        'uid', 'pwd', 'firstName', 'lastName',
        'email', 'primaryRole'
        ]
    # primaryRole of a user has to one of these
    accesslevels = ['Dev', 'PM', 'DH', 'Su', 'Admin']

    # checks if the mandatory fields are provided or not
    for val in requiredinfo:
        if val not in userjson:
            return -1
        elif not len(userjson[val]):
            return -1

    # checks if the primaryRole is available or not
    if userjson['primaryRole'] not in accesslevels:
        return 0

    return 1


def modify_user_check(changesjson):
    if 'uid' not in changesjson:
        return error_codes.incomplete_user_information
    # checks if the user password is also present in changesjson
    if 'primaryRole' in changesjson:
        roles = ['Dev', 'DH', 'PM', 'Su', 'Admin']
        if changesjson['primaryRole'] not in roles:
            return error_codes.incorrect_primaryRole
    elif 'pwd' in changesjson:
        return error_codes.cannot_modify_password
    elif 'activationStatus' in changesjson and \
            not changesjson['activationStatus']:
        return error_codes.call_deactivate_user

    return 200


def filteruseroutput(users):
    filteredusers = []
    outputfields = [
        'uid', 'firstName', 'lastName',
        'email', 'primaryRole', 'nodes',
        'activationStatus'
    ]
    for user in users:
        newuser = {}
        for field in outputfields:
            if field in user:
                newuser[field] = user[field]
        filteredusers.append(newuser)
    return filteredusers
