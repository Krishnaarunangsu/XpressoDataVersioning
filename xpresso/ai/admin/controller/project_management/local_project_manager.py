import os
import urllib
import subprocess
from copy import deepcopy
from shutil import copy2, copytree, rmtree, move

from xpresso.ai.admin.controller.external import bitbucketapi
from xpresso.ai.admin.controller.utils import error_codes
from xpresso.ai.admin.controller.external.jenkins_manager import JenkinsManager
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import  *

config_path = XprConfigParser.DEFAULT_CONFIG_PATH
config = XprConfigParser(config_path)
username = config['bitbucket']['username']
password = config['bitbucket']['password']
escape_password = urllib.parse.quote(password)
skeletonpath = f"http://{username}:{escape_password}@bitbucket.org/"\
               f"abzooba-screpo/skeleton-build.git"

bitbucket = bitbucketapi.bitbucketapi()

logger = XprLogger()


def replace_string(key, replacement, direc):
    print(key)
    print(replacement)
    print(direc)
    for dname, dirs, files in os.walk(direc):
        print(files)
        for fname in files:
            fpath = os.path.join(dname, fname)
            with open(fpath) as f:
                s = f.read()
            s = s.replace(key, replacement)
            print(key)
            print(replacement)
            print(fpath)
            with open(fpath, "w") as f:
                f.write(s)
    print("String replace comleted")


def local_code_setup(reponame, newrepourl):
    if os.path.exists('/tmp/skeleton-build'):
        rmtree('/tmp/skeleton-build')

    if os.path.exists(f'/tmp/{reponame}'):
        rmtree(f'/tmp/{reponame}')

    logger.info("Cloning skeleton-build repo")
    bitbucket.clone_bitbucket_repo(
        skeletonpath, '/tmp/skeleton-build'
    )
    logger.info(f"Cloning {reponame}")
    bitbucket.clone_bitbucket_repo(newrepourl, f'/tmp/{reponame}')


def createcomponents(components, reponame, newrepourl, projectname):
    logger.debug("Creating components locally.")
    for component in components:
        component_name = component['name']
        component_type = component['type']
        flavor = component['flavor']
        logger.info(f"\n Adding new component : {component}\n")
        src_flavor_dir = f"/tmp/skeleton-build/{component_type}/{flavor}"
        dst_flavor_dir = f"/tmp/{reponame}/{component_name}"
        copytree(src_flavor_dir, dst_flavor_dir)

        if flavor.lower() == 'python' and component_type == 'service':
            default_dir = f"/tmp/{reponame}/{component_name}/"\
                           f"{{{{XPRESSO_PROJECT_NAME}}}}"
            new_dir = f"/tmp/{reponame}/{component_name}/{component_name}"
            move(default_dir, new_dir)
            replace_string(
                "{{XPRESSO_PROJECT_NAME}}", component_name,
                f"/tmp/{reponame}/{component_name}"
            )

        # create pipeline
        try:
            config = XprConfigParser()
            jenkins_manager = JenkinsManager(config)
            jenkins_manager.create_pipeline(
                f'{projectname}__{component_name}', newrepourl)
            print("pipeline created")
        except:
            print("error in creating pipeline")


def pushrepo(projectjson, repourl):
    """
        pushrepo pushes the code to bitbucket
    """
    try:
        components = projectjson['components']
        name = projectjson['name']
        reponame = name + '_sc'
        logger.debug(f"repourl is : {repourl}")
        bb_split = repourl.split("//")
        bb_split[1] = f"{username}:{escape_password}@"+bb_split[1]
        newrepourl = "//".join(bb_split)
        local_code_setup(reponame, newrepourl)
        dst_makefile_path = f"/tmp/{reponame}/Makefile"
        if not os.path.exists(dst_makefile_path):
            src_makefile_path = f"/tmp/skeleton-build/Makefile"
            copy2(src_makefile_path, dst_makefile_path)
            print("Makefile added")
        createcomponents(components, reponame, newrepourl, name)
        bitbucket.push_repo_to_bitbucket(f"/tmp/{reponame}")
        rmtree('/tmp/skeleton-build')
        rmtree(f'/tmp/{reponame}')
        return True
    except Exception as e:
        print("caught exception.: ", e)
        return False


def setup_project(project_json):
    creation_response = bitbucket.create_bitbucket_project(project_json)
    return_response = {
        "status": 200,
        "project_json": project_json
    }
    if creation_response["type"] == "error":
        return_response['status'] = error_codes.project_creation_failed
        return return_response

    repo_json = bitbucket.create_bitbucket_repo(project_json)
    if repo_json['type'] == 'error':
        return_response['status'] = error_codes.repo_creation_failed
        return return_response

    repourl = deepcopy(repo_json['links']['html']['href'])+'.git'
    pushrepo_code = pushrepo(project_json, repourl)
    if not pushrepo_code:
        return_response['status'] = error_codes.project_push_failed
        return return_response
    # repourl = repojson['links']['clone'][0]['href']
    project_json['giturl'] = deepcopy(repo_json['links']['html']['href'])
    return_response['project_json'] = project_json
    return return_response


def modify_project_locally(projectinfo, changesjson):
    if 'components' in changesjson:
        repourl = deepcopy(projectinfo['giturl'])+'.git'
        pushrepo_code = pushrepo(changesjson, repourl)
        if not pushrepo_code:
            return error_codes.project_push_failed
    else:
        pass
    return 200
