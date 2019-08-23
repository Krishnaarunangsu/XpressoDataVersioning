import os
import datetime
import re
import pickle
import json



from exception_handling.custom_exception import *
from data_versioning.pachyderm_client import PachydermClient
#from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
# import data_exploration.dataset as datasetmodule

# from xpresso.ai.core.logging.xpr_log import XprLogger
from exception_handling.custom_exception import *
from data_versioning.pachyderm_client import PachydermClient
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
# import xpresso.ai.core.data.dataset as datasetmodule
# Krishna



class PachydermRepoManager:
    """
    Manages repos on pachyderm cluster
    """

    # pachyderm config variables
    PACHYDERM_CONFIG = "pachyderm_server"
    HOST_ADDRESS = "cluster_ip"
    PORT = "port"
    # user input variables
    BRANCH_NAME = "branch_name"
    COMMIT_ID = "commit_id"
    DATASET_NAME = "dataset_name"
    DESCRIPTION = "description"
    LIST = "list"
    PATH = "path"
    PULL = "pull"
    REPO_NAME = "repo_name"
    # Internal Code specific variables
    ACCEPTED_FIELD_NAME_REGEX = r"[\w, -]+$"
    DEFAULT_LIST_DATASET_PATH = "/"
    PICKLE_FILE_EXTENSION = ".pkl"
    PUSH_FILES_MANDATORY_FIELDS = ["repo_name", "branch_name",
                                   "dataset_name", "path", "description"]
    # output variables
    REPO_OUTPUT_NAME = "name"
    BRANCH_OUTPUT_NAME = "branch"
    OUTPUT_DATE_STR_FORMAT = "%d/%m/%y"
    CREATION_DATE = "Date of creation"
    OUTPUT_DATASET_FIELD = "dataset"
    FILE_TYPE = "type"
    OUTPUT_COMMIT_FIELD = "commit"
    OUTPUT_COMMIT_ID = "id"

    def __init__(self):
        # self.logger = XprLogger()

        #self.config = XprConfigParser(config_path)[self.PACHYDERM_CONFIG]
        try:
            self.pachyderm_client = self.connect_to_pachyderm()
        except PachydermOperationException as err:
            raise ValueError(err)


    def connect_to_pachyderm(self):
        """
        connects to pachyderm cluster and returns a PfsClient connection instance

        :return:
            returns a PfsClient Object
        """
        client = PachydermClient(
            "172.16.3.51",
            30650
           # self.config[self.HOST_ADDRESS],
           # self.config[self.PORT]
        )
        print('Jagannath')
        return client

    def create_repo(self, repo_json):
        """
        creates a new repo on pachyderm cluster
        :param repo_json:
            Information of the repo to be created
            keys - repo_name, description
                repo_name: name of the repo
                description: description of repo
        :return:
        """
        if self.REPO_NAME not in repo_json:
            raise RepoNotProvidedException()
        if not self.name_validity_check(repo_json[self.REPO_NAME]):
            raise PachydermFieldsNameException()
        description = ""

        print(self.REPO_NAME)

        if self.DESCRIPTION in repo_json:
            description = repo_json[self.DESCRIPTION]

        self.pachyderm_client.create_new_repo(
            repo_json[self.REPO_NAME], description
        )

        return

    def get_repos(self):
        """
        returns the list of all available repos

        :return:
            returns list of repos
        """
        repo_info = self.pachyderm_client.get_repo()
        return self.filter_repo_output(repo_info)

    def check_repo_existence(self, repo_name):
        """
        checks if the repo exists or not

        :return:
            bool: True or False
        """
        exist_status = False
        repo_list = self.get_repos()
        for repo in repo_list:
            if repo[self.REPO_OUTPUT_NAME] == repo_name:
                exist_status = True
                break
        return exist_status

    def check_branch_existence(self, repo_name, branch_name):
        """
        checks if the repo exists or not

        :return:
            bool: True or False
        """
        exist_status = False
        branch_list = self.get_branches(repo_name)
        return branch_name in branch_list

    def delete_repo(self, repo_name):
        """
        deletes a repo from pachyderm cluster

        This is admin level operation
        :param repo_name:
            name of the repo to be deleted
        :return:
            no return statement
        """
        if not self.check_repo_existence(repo_name):
            raise RepoNotProvidedException(f"{repo_name} repo is not present")
        self.pachyderm_client.delete_repo(repo_name)

    def create_branch(self, branch_info):
        """
        creates a new branch in specified repo on pachyderm cluster

        :param branch_info:
            info of the branch to be created
            keys - repo_name, branch_name
                repo_name: name of the repo
                branch_name: name of the branch
        :return:
        """
        if self.REPO_NAME not in branch_info or \
                self.BRANCH_NAME not in branch_info:
            raise BranchInfoException("Repo name & branch name is required")

        if not self.name_validity_check(branch_info[self.BRANCH_NAME]):
            raise PachydermFieldsNameException(
                f"Invalid format for {branch_info[self.BRANCH_NAME]}"
            )

        repo_status = self.check_repo_existence(branch_info[self.REPO_NAME])
        if not repo_status:
            raise BranchInfoException(
                f"Unable to find a repo `{branch_info[self.REPO_NAME]}`"
            )

        self.pachyderm_client.create_new_branch(
            branch_info[self.REPO_NAME],
            branch_info[self.BRANCH_NAME]
        )

    def get_branches(self, repo_name):
        """
        returns a list of branches in specified repo

        :param repo_name:
            name of the repo to list the branches
        """
        if not self.check_repo_existence(repo_name):
            raise RepoNotProvidedException(f"{repo_name} repo not found")

        branch_info = self.pachyderm_client.get_branch(repo_name)
        return self.filter_branch_output(branch_info)

    def delete_branch(self, repo_name, branch_name):
        """
        deletes a branch from the specified repo

        :param repo_name:
            name of the repo branch is in
        :param branch_name:
            name of the branch that needs to be deleted
        """
        if not self.check_branch_existence(repo_name, branch_name):
            raise BranchInfoException("Incorrect info for deleting branch")
        self.pachyderm_client.delete_branch(repo_name, branch_name)

    def list_commit(self, repo_name, branch_name):
        """
        lists commits in a repo

        :param repo_name:
            name of the repo
        :param branch_name:
            name of the branch
        :return:
            list of commits
        """
        # check for both repo and branch is done in check_branch_existence
        if not self.check_branch_existence(repo_name, branch_name):
            raise BranchInfoException("Invalid input info to list commits")

        commit_info = self.pachyderm_client.list_commit(repo_name)
        commit_list = []
        for commit_item in commit_info:
            commit_data = self.filter_commit_info(commit_item)
            if commit_data[self.BRANCH_OUTPUT_NAME] == branch_name:
                commit_list.append(commit_data)

        return commit_list

    def push_files(self, files_info):
        """
        pushes file/files into a pachyderm cluster

        :param files_info:
            (Dict) info of files, repo & branch
            keys - repo_name, branch_name, dataset_name, path, description
                repo_name: name of the repo
                branch_name: name of the branch
                dataset_name: name of dataset that will be pushed to cluster
                path: local path of file or list of files i.e a directory
                description: A brief description on this push
            All of the above fields are mandatory
        """
        for field in self.PUSH_FILES_MANDATORY_FIELDS:
            if field not in files_info:
                raise DatasetInfoException(f"'{field}' field not provided")
            elif field != self.PATH and not self.name_validity_check(files_info[field]):
                # path can have '/'. Hence excluded from this check
                raise PachydermFieldsNameException(f"Invalid {field} value")

        if not os.path.exists(files_info[self.PATH]):
            raise DatasetPathException(f"path {files_info[self.PATH]} is invalid")
        elif os.path.isfile(files_info[self.PATH]):
            dataset_dir = os.path.dirname(os.path.abspath(files_info[self.PATH]))
        else:
            dataset_dir = files_info[self.PATH]

        # fetches the path of all the files inside the dataset directory
        file_list = self.fetch_file_path_list(dataset_dir,
                                              files_info[self.DATASET_NAME])
        new_commit_id = self.pachyderm_client.push_dataset(
            files_info[self.REPO_NAME],
            files_info[self.BRANCH_NAME],
            file_list,
            files_info[self.DESCRIPTION]
        )

        return new_commit_id

    def manage_xprctl_dataset(self, method, data_info):
        """

        :param method:
            method to be called
        :param data_info:
            (Dict) info of the dataset
            keys - repo_name, branch_name, path, commit_id
                repo_name: name of the repo
                branch_name: name of the branch
                path(Optional): path at which dataset is saved on
                                pachyderm cluster
                commit_id: id of the commit from which dataset can be pulled
            Either one of branch_name or commit_id must be provided
        :return:
            returns appropriate output of list or path
        """
        if self.REPO_NAME not in data_info:
            raise RepoNotProvidedException()
        elif self.BRANCH_NAME not in data_info and self.COMMIT_ID not in data_info:
            raise DatasetInfoException(
                f"Either one of branch name or commit id is required"
            )

        path = self.DEFAULT_LIST_DATASET_PATH
        if self.PATH in data_info:
            path = data_info[self.PATH]

        branch_name = None
        if self.BRANCH_NAME in data_info:
            branch_name = data_info[self.BRANCH_NAME]

        commit_id = None
        if self.COMMIT_ID in data_info:
            commit_id = data_info[self.COMMIT_ID]

        if method == self.PULL:
            return self.pull_dataset(data_info[self.REPO_NAME],
                                     branch_name,
                                     path,
                                     commit_id)
        else:
            return self.list_dataset(data_info[self.REPO_NAME],
                                     branch_name,
                                     path,
                                     commit_id)


    def pull_dataset(self, repo_name, branch_name, path="/", commit_id=None):
        """
        pulls a dataset from pachyderm cluster and load it locally

        :param repo_name:
            name of the repo
        :param branch_name:
            name of the branch
        :param path:
            (Optional) path of the dir in which dataset might be present
                       path of the dataset
        :param commit_id:
            (Optional) id of the commit
        :return:
            returns the path of the directory where dataset is saved
        """
        dataset_list = self.list_dataset(repo_name, branch_name, path, commit_id)
        current_dir = os.getcwd()
        new_dir_path = os.path.join(
            current_dir,
            dataset_list[self.OUTPUT_COMMIT_FIELD][self.OUTPUT_COMMIT_ID])
        if len(dataset_list[self.OUTPUT_DATASET_FIELD]):
            if not os.path.exists(new_dir_path):
                os.makedirs(new_dir_path)

        for file_info in dataset_list[self.OUTPUT_DATASET_FIELD]:
            if file_info[self.FILE_TYPE] == "File":
                file_in_bytes = self.pachyderm_client.pull_dataset(
                    repo_name,
                    dataset_list[self.OUTPUT_COMMIT_FIELD][self.OUTPUT_COMMIT_ID],
                    file_info[self.PATH]
                )
                # This needs to be done because path in file_info contains pachyderm path
                # It always starts with / . Hence it needs to be excluded exclusively
                file_info[self.PATH] = file_info[self.PATH].lstrip("/")
                local_write_path = os.path.join(new_dir_path, file_info[self.PATH])
                self.write_to_file(path=local_write_path, content=file_in_bytes)
        return new_dir_path

    def list_dataset(self, repo_name, branch_name, path="/", commit_id=None):
        """
        list of dataset as per provided information

        :param repo_name:
            name of the repo
        :param branch_name:
            name of the branch
        :param path:
            (Optional) path of the dir in which dataset might be present
        :param commit_id:
            (Optional) id of commit to fetch dataset from
        :return:
            returns a dict with file and commit info
        """
        # verifies provided info and returns a dict with request input fields
        data_info = self.verify_dataset_info(repo_name, branch_name,
                                             path, commit_id)
        list_output = {}
        dataset_list = self.pachyderm_client.list_dataset(
            data_info[self.REPO_NAME], data_info[self.COMMIT_ID],
            data_info[self.PATH]
        )
        list_output[self.OUTPUT_DATASET_FIELD] = dataset_list

        commit = self.pachyderm_client.inspect_commit(
            data_info[self.REPO_NAME], data_info[self.COMMIT_ID])
        list_output[self.OUTPUT_COMMIT_FIELD] = self.filter_commit_info(commit)
        return list_output

    def delete_dataset(self, repo_name, commit_id, path="/"):
        """
        deletes a dataset from the pachyderm cluster

        :param repo_name:
            name of the repo dataset is in
        :param path:
            (Optional)path at which dataset is saved
        :param commit_id:
            id of the commit in which dataset is added
        :return:
        """
        self.pachyderm_client.delete_dataset(repo_name, commit_id, path)

    def verify_dataset_info(self, repo_name, branch_name, path, commit_id):
        """
        verifies if the provided info for a dataset is valid or not

        :param repo_name:
            name of the repo dataset is in
        :param branch_name:
            name of the branch dataset is in
        :param path:
            path of the dataset
        :param commit_id:
            id of the commit
        :return:
            returns updated info else throws an exception
            in case of error
        """
        data_info = {}

        if not repo_name:
            raise RepoNotProvidedException()
        data_info[self.REPO_NAME] = repo_name

        if not commit_id and not branch_name:
            raise DatasetInfoException(
                "Either one of commit_id or branch_name is mandatory"
                )
        elif commit_id:
            commit_info = self.pachyderm_client.inspect_commit(
                repo_name,
                commit_id
            )

            if branch_name and commit_info.branch.name != branch_name:
                raise DatasetInfoException(
                    "commit_id and branch_name info conflicts"
                    )

            data_info[self.COMMIT_ID] = commit_id
            data_info[self.BRANCH_NAME] = commit_info.branch.name
        else:
            branch_info = self.pachyderm_client.inspect_branch(
                repo_name,
                branch_name
            )
            data_info[self.BRANCH_NAME] = branch_name
            data_info[self.COMMIT_ID] = branch_info.head.id

        data_info[self.PATH] = self.DEFAULT_LIST_DATASET_PATH
        if path:
            data_info[self.PATH] = path

        return data_info

    def fetch_file_path_list(self, dataset_dir, dataset_name):
        """
        fetches the list of file paths recursively in a directory

        :param dataset_dir:
            path of the dataset directory
        :param dataset_name:
            name of the dataset
        :return:
            returns a list of file paths inside the dataset directory
        """
        file_list = []
        pachyderm_destination_path = f"dataset/{dataset_name}"

        for dir_path, dirs, files in os.walk(dataset_dir):
            for file in files:
                file_path = os.path.join(dir_path, file)
                destination_path = self.create_pachyderm_path(
                    file_path, dataset_dir, pachyderm_destination_path)
                file_list.append((file_path, destination_path))

        return file_list

    @staticmethod
    def create_pachyderm_path(local_file_path, input_dataset_path,
                              dir_path_on_pachyderm):
        """
        takes the path of file on local system and generates new path for it
        on pachyderm cluster

        :param local_file_path:
            file path on the local system
        :param input_dataset_path:
            path of the input dataset provided
        :param dir_path_on_pachyderm:
            path of directory on pachyderm cluster inside which new file
            should be added
        :return:
        """
        rel_file_path = os.path.relpath(local_file_path, input_dataset_path)
        pachyderm_local_path = os.path.join(dir_path_on_pachyderm,
                                            rel_file_path)
        return pachyderm_local_path

    @staticmethod
    def filter_commit_info(commit_info_object):
        """
        takes CommitInfo object and outputs a user friendly output

        :param commit_info_object:
            CommitInfo object
        :return:
            commit information as a dict
        """
        commit_info = {
            "id": "",
            "repo": "",
            "branch": "",
            "description": ""
        }
        commit_info["id"] = commit_info_object.commit.id
        commit_info["repo"] = commit_info_object.commit.repo.name
        commit_info["description"] = commit_info_object.description
        commit_info["branch"] = commit_info_object.branch.name
        # TODO: Add Start and End time in next update
        return commit_info

    def filter_repo_output(self, repo_info):
        """
        filters RepoInfo object and returns user friendly output

        :param repo_info:
            RepoInfo object
        :return:
            list of repos
        """
        repo_list = []
        for repo_item in repo_info:
            temp_repo = {}
            temp_repo[self.REPO_OUTPUT_NAME] = repo_item.repo.name
            temp_repo[self.CREATION_DATE] = self.find_date_from_seconds(
                repo_item.created.seconds
            )
            repo_list.append(temp_repo)
        return repo_list

    def find_date_from_seconds(self, number_of_seconds):
        """
        Takes number of seconds as input and outputs date from 1970/1/1

        :param number_of_seconds:
            number of seconds
        :return:
            date as string
        """
        start_date = datetime.datetime(1970, 1, 1)
        new_date_object = start_date + datetime.timedelta(seconds=number_of_seconds)
        new_date = new_date_object.strftime(self.OUTPUT_DATE_STR_FORMAT)
        return new_date

    @staticmethod
    def filter_branch_output(branch_info):
        """
        takes a branch info object and returns filtered output

        :param branch_info:
            BranchInfo object
        :return:
            list of branches filtered
        """
        branch_list = []
        for branch in branch_info:
            branch_list.append(branch.name)
        return branch_list

    def write_to_file(self, path, content):
        """
        writes the contents of dataset to a local file

        :param path:
            local path of the file
        :param content:
            file content
        """
        if os.path.isfile(os.path.dirname(path)):
            # TODO: Remove files that are added already to local system
            raise LocalFilePathException(f"file exists at this path; {path}")

        file_extension = os.path.splitext(path)[1]
        if not os.path.exists(path) and \
                not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))

        with open(path, "wb") as out_file:
            if file_extension == self.PICKLE_FILE_EXTENSION:
                # if it is pickle file we use pickle to dump it onto file
                dataset_object = pickle.loads(content)
                pickle.dump(dataset_object, out_file, pickle.HIGHEST_PROTOCOL)
            else:
                out_file.write(content)
        return

    def name_validity_check(self, name):
        """
        Checks if the name provided contains only alphanumeric characters,
        underscore or dashes

        :param name:
            (str) : name
        :return:
            check status i.e. True or False
        """
        accepted_pattern = self.ACCEPTED_FIELD_NAME_REGEX
        if not isinstance(name, str):
            raise PachydermFieldsNameException()
        match = re.match(accepted_pattern, name)
        if not match:
            return False
        return True


if __name__ == '__main__':
    pc =PachydermRepoManager()
    print(pc)
    with open('../resources/create_repo.json', 'rb') as f:
        data = json.load(f)
        print(data)
        pc.create_repo(data)
        #process_json(data)
