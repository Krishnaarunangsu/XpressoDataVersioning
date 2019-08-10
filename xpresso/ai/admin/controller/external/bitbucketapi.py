import subprocess
from copy import deepcopy

import requests

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *

projects = "projects/"
teams = "teams/"
repos = "repositories/"


class bitbucketapi():
    """
        bitbucketapi class defines methods to work with bitbucket repos

        This class is used in creating and updating repos on bitbucket.
        bitbucket standard RestAPI 2.0 has been used for creating project
        and repository. Standard git commands through subprocess are used
        in cloning & updating the repository

        ....

        Methods
        -------
            exec_command()
                executes a linux command through subprocess

            create_bitbucket_project()
                creates a project on bitbucket using RESTAPI

            create_bitbucket_repo()
                creates a repo on bitbucket using RESTAPI

            clone_bitbucket_repo()
                clones a repo on bitbucket using git command
                through subprocess

            push_bitbucket_repo()
                pushes updated code to bitbucket using git
                command through subprocess
    """
    config_path = XprConfigParser.DEFAULT_CONFIG_PATH
    logger = XprLogger()

    def __init__(self):
        self.logger = XprLogger()
        self.config = XprConfigParser(self.config_path)
        self.defaulturl = self.config['bitbucket']['restapi']
        self.teamname = self.config['bitbucket']['teamname']
        self.username = self.config['bitbucket']['username']
        self.password = self.config['bitbucket']['password']

        # Following project format provided for bibucket RESTAPI
        self.defaultprojectbody = {
            "name": "",
            "description": "",
            "key": "",
            "is_private": False
        }
        # Following repo format provided for bibucket RESTAPI
        self.defaultrepobody = {
            "scm": "git",
            "project": {
                "key": ""
            }
        }

    def exec_command(self, command:list, inputflag: bool,
                     inputcmd: str, wd: str) -> bool:
        """
            exec_command executes a input command through subprocess

            Takes command and current working directory
            to execute the command there. Also takes the argument
            for prompt input in some cases

            ....

            Parameters
            ----------
                command -> input command to be executed
                inputflg -> flag to specify if any input prompt is present
                inputcmd -> input prompt in case required
                wd -> working directory where the command needs to be
                        executed

            Returns
            -------
                returns a True or False boolean based on execution status
        """
        # executes the command at specified working directory path
        self.logger.info(f"Execute Command :{command} @ {wd}")
        exec = subprocess.Popen(command,
                                cwd=wd,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE
                                )

        # Incase input prompt is expected and provided
        if inputflag and inputcmd:
            # subprocess Popen returns a stdin filestream
            # input string has to be convereted to bytes before
            # writing to the stream
            inputbytes = str.encode(inputcmd + '\n')
            # Providing input for ex password to the stdin stream
            exec.stdin.write(inputbytes)
            exec.stdin.flush()

        # returncode will be None until the process is complete
        # provide timeout case to prevent forever loop
        while exec.returncode is None:
            print("waiting")
            self.logger.info("Waiting for the command execution to end")
            exec.wait()

        # once process is complete returncode will be 0 if execution succeeds
        if exec.returncode != 0:
            stderror = exec.stderr.readlines()
            print(stderror)
            self.logger.error(f"\n Error in command execution: \n {stderror}")
            return False
        else:
            self.logger.info("Command successfully executed")
            return True

    def create_bitbucket_project(self, projectjson: dict) -> dict:
        """
            creates a project on bitbucket

            creates a project on bitbucket through RESTAPI.
            using bitbucket api v2.0. Project info is provided
            as body in post request

            ....
            Parameters
            ----------
                projectjson
                    information on project to be created

            Returns
            -------
                returns the status code of post request

        """
        body = deepcopy(self.defaultprojectbody)
        body['name'] = projectjson['name']
        body['description'] = projectjson['projectDescription']
        body['key'] = projectjson['name'] + '_xpr'
        # Team name should be provided.
        create_project_url = self.defaulturl + teams + self.teamname + projects
        self.logger.debug(f"New project creation url: {create_project_url}")
        self.logger.debug(f"project body : {body}")
        projectcreation = requests.post(
            create_project_url, json=body, auth=(self.username, self.password)
        )
        print("projectcreation is ", projectcreation.text)
        self.logger.info(f"projectcreation response is {projectcreation.text}")
        return projectcreation.json()

    def create_bitbucket_repo(self, projectjson: dict) -> dict:
        """
            creates a repo on bitbucket

            creates a repository on bitbucket through RESTAPI.
            using bitbucket api v2.0. Project info is provided
            as body in post request

            ....
            Parameters
            ----------
                projectjson
                    same project information is used in creating
                    the repository

            Returns
            -------
                returns response json of repocreation. The json
                contain links & info to the repository.
        """
        body = deepcopy(self.defaultrepobody)
        reponame = projectjson['name'] + '_sc'
        body['project']['key'] = projectjson['name'] + '_xpr'
        create_repo_url = self.defaulturl + repos + self.teamname + reponame
        self.logger.debug(f"New repo creation url: {create_repo_url}")
        self.logger.debug(f"repo body : {body}")
        repocreation = requests.post(
            create_repo_url, json=body, auth=(self.username, self.password)
        )
        print("\n repocreation is : ", repocreation.text)
        self.logger.info(f"repocreation response is {repocreation.text}")
        return repocreation.json()

    def clone_bitbucket_repo(self, clone_link: str,
                             local_clone_path: str) -> int:
        """
            Clones a repo from bitbucket to local system.

            clones a repository to the specified path in the
            input argument. uses git commands through subprocess
            to clone the repo.

            ....
            Parameters
            ----------
                clone_link
                    bitbucket link to the repository

                local_clone_path
                    path on local server where the repo
                    needs to be cloned
            Returns
            -------
                returns the status code of cloning the repo

        """
        clone_command = [
            'git',
            'clone',
            clone_link,
            local_clone_path
        ]
        self.logger.info(f"Cloning {clone_link} to {local_clone_path}")
        # exec_command internally calls subprocess to clone the repo
        clone_repo_status = self.exec_command(clone_command, False, None, None)
        if not clone_repo_status:
            self.logger.info("Cloning failed")
            raise BitbucketCloneException("Cloning failed.")

    def push_repo_to_bitbucket(self, remotepath: str) -> bool:
        """
            pushes the repository to bitbucket

            After updating the code, the repository is pushed
            to bitbucket using git commands

            ....
            Parameters
            ----------
                remotepath
                    path of the repository on the local server

            Returns
            -------
                returns the status push request

        """
        # reduce add and commit to single call
        gitaddcmd = ["git", "add", "-A"]
        gitcommitcmd = ["git", "commit", "-m", "Initial commit"]
        gitpushcmd = ["git", "push", "-u", "origin", "master"]
        for gitcmd in [gitaddcmd, gitcommitcmd, gitpushcmd]:
            gitstatus = self.exec_command(gitcmd, False, "", wd=remotepath)
            if not gitstatus:
                return False
            self.logger.info(f"{' '.join(gitcmd)} : Done")
        return True

    def delete_bitbucket_repo(self, repo):
        pass
