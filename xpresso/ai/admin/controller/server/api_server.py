import os
import json
from flask import Flask, request, jsonify, render_template, session
from datetime import datetime

from xpresso.ai.admin.controller.authentication.authenticationmanager \
    import AuthenticationManager
from xpresso.ai.admin.controller.authentication.sso_manager import SSOManager
from xpresso.ai.admin.controller.cluster_management.xpr_clusters \
    import XprClusters
from xpresso.ai.admin.controller.metrics.metrics_aggregator import \
    MetricsAggregator
from xpresso.ai.admin.controller.metrics.promotheus_metric_report_generator import \
    PromotheusMetricReportGenerator
from xpresso.ai.admin.infra.packages.package_manager import PackageManager, \
    ExecutionType
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.user_management.usermanager import UserManager
from xpresso.ai.admin.controller.node_management.nodemanager import NodeManager
from xpresso.ai.admin.controller.project_management.xpr_project_manager import \
    XprProjectManager
from xpresso.ai.admin.controller.project_management.xpr_project_build \
    import XprProjectBuild
from xpresso.ai.admin.controller.project_management.xpr_project_deployment \
    import XprProjectDeployment
from xpresso.ai.core.utils.generic_utils import get_version
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.persistence.mongopersistencemanager \
    import MongoPersistenceManager
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.utils.xprresponse import XprResponse
from xpresso.ai.admin.controller.pachyderm_repo_management.pachyderm_repo_manager \
    import PachydermRepoManager

logger = XprLogger()

# Default response for permission_denied error
permission_denied_code = error_codes.permission_denied
permission_denied = XprResponse('failure', permission_denied_code,
                                {"message": "Permission Denied"})

error_response_path = '/opt/xpresso.ai/config/error_response.json'
try:
    with open(error_response_path, 'r', encoding='utf-8') as json_file:
        error_response = json.load(json_file)
except FileNotFoundError:
    error_response = {}

MONGO_SECTION = 'mongodb'
URL = 'mongo_url'
DB = 'database'
UID = 'mongo_uid'
PWD = 'mongo_pwd'
W = 'w'
config_path = XprConfigParser.DEFAULT_CONFIG_PATH
config = XprConfigParser(config_path)

mongo_persistence_manager = MongoPersistenceManager(
    url=config[MONGO_SECTION][URL],
    db=config[MONGO_SECTION][DB],
    uid=config[MONGO_SECTION][UID],
    pwd=config[MONGO_SECTION][PWD],
    w=config[MONGO_SECTION][W])

server_error_response = XprResponse("failure", error_codes.server_error,
                                    {"message": "Unknown Server Error"})

app = Flask(__name__,
            template_folder=os.path.join(os.getcwd(), 'templates'),
            static_folder=os.path.join(os.getcwd(), 'templates/static'))
app.secret_key = "s;djnf2DSFKAL23))$&)@LJSDO@_!@#!_DSAD__#%*_!@_#"


@app.route('/auth', methods=['POST', 'DELETE'])
def handle_auth_requests():
    authentication_manager = AuthenticationManager(mongo_persistence_manager)
    request_type = None
    response = server_error_response
    try:
        if request.method == 'POST':
            logger.info('Received login request')
            request_type = 'Login'
            credentials = get_json_data()
            response = authentication_manager.login(credentials)
        elif request.method == 'DELETE':
            request_type = 'Logout'
            logger.info('Received logout request.')
            token = request.headers['token']
            response = authentication_manager.logout(token)

    except (XprExceptions, KeyError, ValueError) as e:
        logger.error('{} request failed'.format(request_type))
        logger.error(type(e))
        response = XprResponse("failure", e.error_code,
                               {"message": e.message})

    finally:
        logger.info('Exiting with response {}'.format(response))
        return jsonify(response.__dict__)


@app.route('/clusters', methods=['POST', 'GET', 'DELETE'])
def handle_cluster_requests():
    xpr_clusters = XprClusters(mongo_persistence_manager)
    response = server_error_response
    try:
        json_input = get_json_data()
        validate_request('Admin')
        if request.method == 'GET':  # get clusters
            logger.info('get_cluster request received')
            cluster_info = xpr_clusters.get_clusters(json_input)
            response = XprResponse('success', None, cluster_info)
        elif request.method == 'DELETE':  # deactivate cluster
            logger.info("deactivate_cluster request received")
            xpr_clusters.deactivate_cluster(json_input)
            response = XprResponse('success', None,
                                   {"message": "Cluster Deactivated"})
        elif request.method == 'POST':  # register cluster
            logger.info('register_cluster request received.')
            cluster_info = xpr_clusters.register_cluster(json_input)
            response = XprResponse('success', None, cluster_info)
    except (XprExceptions, KeyError, ValueError) as e:
        logger.error('Cluster request failed')
        response = XprResponse("failure", e.error_code,
                               {"message": e.message})

    finally:
        logger.info('Exiting with response {}'.format(response.__dict__))
        return jsonify(response.__dict__)


@app.route('/users', methods=['POST', 'GET', 'PUT', 'DELETE'])
def handle_user_requests():
    user_manager = UserManager(mongo_persistence_manager)
    json_data = get_json_data()
    response = server_error_response
    try:
        if request.method == 'POST':
            validate_request('Admin')
            user_manager.register_user(json_data)
            response = XprResponse("success", None,
                                   {"message": "User registered completed"})

        elif request.method == 'GET':
            validate_request('Admin')
            users = user_manager.get_users(json_data)
            response = XprResponse("success", None, users)
        elif request.method == 'PUT':
            validate_modify_user_request(json_data)
            user_manager.modify_user({'uid': json_data['uid']}, json_data)
            response = XprResponse("success", None,
                                   {"message": "User modification completed"})
        else:
            validate_request('Admin')
            if 'uid' in json_data:
                user_manager.deactivate_user(json_data['uid'])
                response = XprResponse(
                    "success", None,
                    {
                        "message": f"{json_data['uid']} deactivated"
                    }
                )

    except XprExceptions as e:
        logger.error('Request failed')
        logger.error(type(e))
        try:
            if not e.message:
                msg = error_response["user_management"][e.error_code]
            else:
                msg = e.message
        except KeyError:
            msg = "Operation failed"
        response = XprResponse("failure", e.error_code, {"message": msg})

    finally:
        logger.debug(
            "Exiting with response: {}".format(jsonify(response.__dict__)))
        return jsonify(response.__dict__)


@app.route('/user/pwd', methods=["PUT"])
def handle_update_password():
    user_manager = UserManager(mongo_persistence_manager)
    json_data = get_json_data()
    response = server_error_response
    try:
        validate_modify_user_request(json_data)
        user_manager.update_password(json_data)
        response = XprResponse(
            "success", None,
            {
                "message": "Password updated successfully"
            }
        )
    except XprExceptions as e:
        logger.error('Request failed')
        print(e)
        logger.error(type(e))
        try:
            if not e.message:
                msg = error_response["user_management"][e.error_code]
            else:
                msg = e.message
        except KeyError:
            msg = "Operation failed"
        response = XprResponse("failure", e.error_code, {"message": msg})

    finally:
        logger.debug(
            "Exiting with response: {}".format(jsonify(response.__dict__)))
        return jsonify(response.__dict__)


@app.route('/projects/build', methods=['POST', 'GET'])
def handle_build_requests():
    xpr_build = XprProjectBuild(mongo_persistence_manager)
    response = server_error_response
    try:
        json_input = get_json_data()
        project_info = validate_request_for_projects(json_input)
        if request.method == 'GET':  # get build versions
            logger.info('get_build_versions request received')
            build_versions = xpr_build.get_build_version(project_info)
            response = XprResponse('success', None,
                                   {"Versions": build_versions})
        elif request.method == 'POST':  # build project
            logger.info('build_project request received.')
            build_info = xpr_build.build_project(json_input, project_info)
            response = XprResponse('success', None, build_info)

    except (XprExceptions, KeyError, ValueError) as e:
        logger.error('Build request failed')
        response = XprResponse("failure", e.error_code,
                               {"message": e.message})

    finally:
        logger.info('Exiting with response {}'.format(response.__dict__))
        return jsonify(response.__dict__)


@app.route('/projects/deploy', methods=['POST', 'DELETE'])
def handle_deploy_requests():
    xpr_deploy = XprProjectDeployment(mongo_persistence_manager)
    response = server_error_response.__dict__
    try:
        json_input = get_json_data()
        project_info = validate_request_for_projects(json_input)
        if request.method == 'DELETE':  # undeploy project
            logger.info('undeploy_project request received')
            status = xpr_deploy.undeploy_project(json_input, project_info)
            response = XprResponse('success', None, status).__dict__
        elif request.method == 'POST':  # deploy project
            logger.info('deploy_project request received.')
            status = xpr_deploy.deploy_project(json_input, project_info)
            response = XprResponse('success', None, status).__dict__

    except (XprExceptions, KeyError, ValueError) as e:
        logger.error('deploy/undeploy request failed')
        response = XprResponse("failure", e.error_code,
                               {"message": e.message}).__dict__

    finally:
        logger.info('Exiting with response {}'.format(response))
        return jsonify(response)


@app.route('/nodes', methods=['POST', 'GET', 'PUT', 'DELETE'])
def handle_node_requests():
    node_manager = NodeManager(mongo_persistence_manager)
    json_data = get_json_data()
    response = server_error_response
    try:
        if request.method == 'POST':
            validate_request('Su')
            node_manager.register_node(json_data)
            response = XprResponse("success", None,
                                   {"message": "Node registration completed"})
        elif request.method == 'GET':
            validate_request('Admin')
            users = node_manager.get_nodes(json_data)
            response = XprResponse("success", None, users)

        elif request.method == 'PUT':
            # a user can modify another only if she has admin privileges
            validate_request('Su')
            node_manager.provision_node(json_data)
            response = XprResponse("success", None,
                                   {"message": "Provision node completed"})
        else:
            validate_request('Su')
            if 'address' in json_data:
                node_manager.deactivate_node(json_data['address'])
                response = XprResponse("success", None,
                                       {"message": "Deactivated node"})
    except XprExceptions as e:
        logger.error('Request failed')
        logger.error(type(e))
        try:
            if not e.message:
                msg = error_response["node_management"][e.error_code]
            else:
                msg = e.message
        except KeyError:
            msg = "Operation failed"
        response = XprResponse("failure", e.error_code, {"message": msg})

    finally:
        logger.debug(f"Exiting with response: {jsonify(response.__dict__)}")
        return jsonify(response.__dict__)


@app.route('/projects/manage', methods=['POST', 'GET', 'PUT', 'DELETE'])
def handle_project_management():
    json_data = get_json_data()
    response = server_error_response
    xpr_project_manager = XprProjectManager(mongo_persistence_manager)
    try:
        if request.method == 'POST':
            validate_request('Admin')
            response = xpr_project_manager.create_project(json_data)
        elif request.method == 'GET':
            validate_request('Admin')
            projects = xpr_project_manager.get_projects(json_data)
            response = XprResponse("success", None, projects)
        elif request.method == 'DELETE':
            validate_request('Admin')
            xpr_project_manager.deactivate_project(json_data)
            response = XprResponse("success", None,
                                   {"message": "Deactivated project"})
        elif request.method == 'PUT':
            validate_request('Admin')
            xpr_project_manager.modify_project(json_data)
            response = XprResponse("success", None,
                                   {
                                       "message": "Project modification completed"})

    except (XprExceptions, KeyError, ValueError) as e:
        logger.error('Request failed')
        logger.error(type(e))
        print("\n\n", e, "\n\n")
        try:
            if not e.message:
                msg = error_response["project_management"][e.error_code]
            else:
                msg = e.message
        except KeyError:
            msg = "Operation failed"
        response = XprResponse("failure", e.error_code, {"message": msg})

    finally:
        logger.debug(f"Exiting with response: {jsonify(response.__dict__)}")
        print("response is ", response.__dict__)
        return jsonify(response.__dict__)


@app.route('/assign_node', methods=['PUT'])
def assign_node():
    assign_json = get_json_data()
    response = server_error_response
    try:
        check_access = validate_request('Su')
        if check_access is True:
            node_manager = NodeManager(MongoPersistenceManager)
            node_manager.assign_node(assign_json)
            response = XprResponse("success", None,
                                   {"message": "Assign node completed"})
        else:
            response = permission_denied

    except XprExceptions as e:
        logger.error('Request failed')
        logger.error(type(e))
        response = XprResponse("failure", e.error_code,
                               {"message": e.message})

    finally:
        logger.debug(f"Exiting with response: {jsonify(response.__dict__)}")
        return jsonify(response.__dict__)


@app.route('/update_xpresso', methods=['POST'])
def update_xpresso():
    update_json = get_json_data()
    response = server_error_response
    try:
        check_access = validate_request('Su')
        if check_access is True:
            # Updating all nodes
            branch_name = update_json["branch_name"] \
                if "branch_name" in update_json else "master"
            filter_json = update_json["filter"] \
                if "filter" in update_json else {}
            node_manager = NodeManager(MongoPersistenceManager)
            updated_list = node_manager.update_all_nodes(
                branch_name=branch_name,
                filter_json=filter_json)

            # Updating Current Project
            package_manager = PackageManager()
            package_manager.run(package_to_install="UpdateLocalXpressoPackage",
                                execution_type=ExecutionType.INSTALL)
            # Restart the service after 5 second
            # TODO This is risky. We need a better way to restart the services.
            #  May be se kubernetes to deploy new versions
            os.spawnl(os.P_NOWAIT, "sleep 5; systemctl restart",
                      "xpresso-controller")
            response = XprResponse('success', 200,
                                   {"message": "Update completed",
                                    "updated_nodes": updated_list})
            return jsonify(response.__dict__)
        else:
            response = permission_denied

    except XprExceptions as e:
        logger.error('Request failed')
        logger.error(type(e))
        response = XprResponse("failure", e.error_code,
                               {"message": e.message})

    finally:
        logger.debug(f"Exiting with response: {jsonify(response.__dict__)}")
        return jsonify(response.__dict__)


def get_json_data():
    try:
        jsondata = json.loads(request.get_json())
    except:
        jsondata = request.get_json()
    return jsondata


def validate_request(access_level):
    try:
        token = request.headers['token']
    except KeyError:
        raise TokenNotSpecifiedException()

    authentication_manager = AuthenticationManager(
        mongo_persistence_manager)
    authentication_manager.validate_token(token, access_level)


def validate_request_for_projects(json_input):
    try:
        token = request.headers['token']
    except KeyError:
        raise TokenNotSpecifiedException()

    authentication_manager = AuthenticationManager(
        mongo_persistence_manager)
    project_info = authentication_manager.validate_build_deploy_token(
        token, json_input)
    return project_info


def validate_modify_user_request(changes_json):
    try:
        token = request.headers['token']
    except KeyError:
        raise TokenNotSpecifiedException()

    authentication_manager = AuthenticationManager(mongo_persistence_manager)
    user_manager = UserManager(mongo_persistence_manager)
    users = user_manager.get_users({"token": token})
    print('got user for token')
    if users and len(users) > 0:
        # if user is trying to modify itself, user cannot change her
        # primary role
        # if some other user id trying to modify, she should have Admin access
        if users[0]['uid'] == changes_json['uid']:
            print('user trying to modify herself')
            if 'primaryRole' not in changes_json:
                print('user not trying to modify role')
                return True
            else:
                print('user trying to modify role')
                raise IllegalModificationException(
                    "Role modification not allowed")
        else:
            authentication_manager.validate_token(token, 'Admin')
    else:
        raise UserNotFoundException()


@app.route('/version', methods=['GET'])
def version():
    response = XprResponse('success', 200, {"version": get_version()})
    return jsonify(response.__dict__)


@app.route('/metrics', methods=['GET'])
def metrics():
    logger.info("Generating promotheus metrics")
    metrics_aggr = MetricsAggregator(config_path=XprConfigParser.DEFAULT_CONFIG_PATH)
    metrics_aggr.initialize()
    metric_list = metrics_aggr.get_all_metrics()
    metric_output = PromotheusMetricReportGenerator()
    final_report = metric_output.generate_report(metric_list)
    logger.info("Metirc generation completed")
    return final_report, 200, {'Content-Type': 'text/plain',
                               'charset': 'utf-8'}


@app.route('/sso/authorize', methods=['GET'])
def handle_sso_authorize():
    authentication_manager = AuthenticationManager(
        mongo_persistence_manager)

    validation_token = None
    if "validation_token" in request.args:
        validation_token = request.args.get("validation_token")

    if "access_token" not in session and validation_token:
        return render_template('login.html', validation_token=validation_token)
    elif "access_token" not in session:
        return render_template('login.html')

    try:
        print(session)
        authentication_manager.validate_token(session.get("access_token"),
                                              "Dev")
        if validation_token:
            authorize_sso_login(validation_token,
                                session.get("access_token"))
        return render_template('login_feedback.html', success="User logged in")
    except (IncorrectTokenException, ExpiredTokenException):
        return render_template('login.html',
                               error="Session Expired! Login Again")
    # except Exception as e:
    #     return render_template('login.html', error="Login Failed")


@app.route('/sso/get_authentication_url', methods=['GET'])
def handle_sso_get_authentication_url():
    sso_manager = SSOManager(mongo_persistence_manager)
    token = sso_manager.generate_token()
    server_url = os.path.join(config["controller"]["server_url"],
                              f"sso/authorize?validation_token={token}")
    return jsonify(XprResponse("success", 200,
                               {"url": server_url,
                                "validation_token": token}).__dict__)


@app.route('/sso/validate', methods=['POST'])
def handle_sso_validate_token():
    json_data = get_json_data()
    if "validation_token" not in json_data:
        response = XprResponse("failure", error_codes.wrong_token,
                               {"message": "token invalid"})
        return jsonify(response.__dict__)

    validation_token = json_data["validation_token"]
    sso_manager = SSOManager(mongo_persistence_manager)
    try:
        token_info = sso_manager.validate_token(validation_token)
        response = XprResponse("success", 200,
                               {"token": token_info, "validated": True})
        return jsonify(response.__dict__)
    except IncorrectTokenException as e:
        response = XprResponse("failure", e.error_code,
                               {"message": "Token not validated"})
        return jsonify(response.__dict__)
    except ControllerClientResponseException:
        response = XprResponse("failure", error_codes.wrong_token,
                               {"message": "Token not validated"})
        return jsonify(response.__dict__)
    except Exception:
        response = XprResponse("failure", 500,
                               {"message": "Unable to process request"})
        return jsonify(response.__dict__)


@app.route('/sso/token_login', methods=['POST'])
def handle_sso_token_login():
    try:
        if validate_request("Dev"):
            response = XprResponse("success", 200,
                                   {"message": "User logged in"})
        else:
            response = XprResponse("failure", error_codes.wrong_token,
                                   {"message": "Token Invalid"})
    except (ExpiredTokenException, PermissionDeniedException):
        response = XprResponse("failure", error_codes.wrong_token,
                               {"message": "Token Invalid"})
    return jsonify(response.__dict__)


def authorize_sso_login(validation_token, token):
    sso_manager = SSOManager(mongo_persistence_manager)
    sso_manager.update_token(
        validation_token=validation_token,
        login_token=token)


@app.route('/sso/login', methods=['GET', 'POST'])
def handle_sso_login():
    if request.method == 'POST':
        authentication_manager = AuthenticationManager(
            mongo_persistence_manager)

        username = request.form['username']
        password = request.form['password']
        if not username or not password:
            render_template('login.html',
                            error="Please provide username/password")

        try:
            login_resp = authentication_manager.login(
                {"uid": username, "pwd": password})
            if "access_token" not in login_resp.results:
                raise AuthenticationFailedException("Invalid Credentials")
            session[username] = login_resp.results["access_token"]
            session["access_token"] = login_resp.results["access_token"]
            print(request.args)

            if "validation_token" in request.args:
                validation_token = request.args.get("validation_token")
                authorize_sso_login(validation_token,
                                    login_resp.results["access_token"])
            return render_template('login_feedback.html',
                                   success="User logged in")

        except ControllerClientResponseException:
            return render_template('login.html',
                                   error="Login Failed")

        except (UserNotFoundException, InvalidPasswordException,
                AuthenticationFailedException):
            return render_template('login.html',
                                   error="Invalid Credentials")

    elif request.method == 'GET':
        return render_template('login.html')


@app.route("/repo", methods=["PUT", "POST"])
def handle_repo_requests():
    """
    handles any requests to repo related search

    Methods: create_repo, create_branch, get_repo

    :return:
        returns response with results in case of success
        or failure message
    """
    json_data = get_json_data()
    response = server_error_response
    repo_manager = PachydermRepoManager()
    try:
        if request.method == "POST":
            # Create repo method call
            validate_request("PM")
            repo_manager.create_repo(json_data)
            response = XprResponse("success", None,
                                   {"message": "Repo created Successfully"})
        elif request.method == "GET":
            # Get the list of repos
            validate_request("Dev")
            repos = repo_manager.get_repos()
            response = XprResponse("success", None, repos)
        elif request.method == "PUT":
            # Updates a repo by creating new branch
            validate_request("Dev")
            repo_manager.create_branch(json_data)
            response = XprResponse("success", None,
                                   {"message": "Branch created successfully"})
    except XprExceptions as e:
        if not e.message:
            msg = error_response["repo_management"][e.error_code]
        else:
            msg = e.message
        response = XprResponse("failure", e.error_code, {"message": msg})
    finally:
        logger.debug(
            "Exiting with response: {}".format(jsonify(response.__dict__)))
        return jsonify(response.__dict__)


@app.route("/dataset/manage", methods=["GET", "POST"])
def handle_dataset():
    """
    handles permission requests to push or pull dataset

    :return:
        returns if access is available or not
    """
    response = XprResponse("success", None, {"message": "Access granted"})
    try:
        if request.method == "POST":
            # Called to push dataset to a repo
            validate_request("Dev")
        elif request.method == "GET":
            # Called to retrieve dataset for a commit
            validate_request("Dev")
        else:
            response = XprResponse("failure", None,
                                   {"message": "Invalid request method"})
    except XprExceptions as e:
        logger.error(type(e))
        response = XprResponse("failure", e.error_code, {"message": e.message})

    finally:
        return jsonify(response.__dict__)


@app.route("/dataset/list", methods=["GET"])
def list_dataset():
    """
    handles permission request to list dataset

    :return:
        returns if access is available or not
    """
    response = XprResponse("failure", None, {"message": "Access Denied"})
    try:
        if request.method == "GET":
            # Called to retrieve dataset for a commit
            validate_request("Dev")
            response = XprResponse("success", None, {"message": "Access granted"})
    except XprExceptions as e:
        logger.error(type(e))
        response = XprResponse("failure", e.error_code, {"message": e.message})

    finally:
        return jsonify(response.__dict__)


@app.errorhandler(404)
def route_not_found(error):
    logger.info(error)
    response = XprResponse("failure", error_codes.server_error,
                           {"message": "Invalid Request Type"})
    return jsonify(response.__dict__)


@app.errorhandler(405)
def method_not_allowed(error):
    logger.info(error)
    response = XprResponse("failure", error_codes.permission_denied,
                           {"message": "Method not allowed"})
    return jsonify(response.__dict__)


@app.errorhandler(500)
def internal_server_error(error):
    logger.info(error)
    response = server_error_response
    return jsonify(response.__dict__)


@app.before_request
def start_request_timer():
    request.start_time = datetime.utcnow()


@app.after_request
def update_event(response_data):
    user_info = {}
    try:

        token = request.headers['token']
        users = UserManager(mongo_persistence_manager).get_users(
            {"token": token})
        if users and len(users):
            user_info["uid"] = users[0]["uid"]
    except KeyError as err:
        logger.error(err)
    current_time = datetime.utcnow()
    event_json = {
        "time": request.start_time,
        "end_time": current_time,
        "processing_time": (current_time - request.start_time).total_seconds(),
        "request_json": get_json_data(),
        "response": response_data.get_json(),
        "request_type": request.path,
        "user": user_info
    }
    mongo_persistence_manager.insert("events", event_json, False)
    return response_data


if __name__ == "__main__":
    app.run(debug=True, port='5050', host='0.0.0.0', use_reloader=False)
