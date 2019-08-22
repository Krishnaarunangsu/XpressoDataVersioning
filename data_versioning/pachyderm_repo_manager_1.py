import os
import datetime
import re
import pickle

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.admin.controller.pachyderm_repo_management.pachyderm_client \
    import PachydermClient
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
import xpresso.ai.core.data.dataset as datasetmodule


class PachydermRepoManager:
    """
    Manages repos on pachyderm cluster
    """
    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH):
        self.logger = XprLogger()
        self.config = XprConfigParser(config_path)["pachyderm"]
        self.pachyderm_client = self.connect_to_pachyderm()

    def connect_to_pachyderm(self):
        """
        connects to pachyderm cluster and returns a PfsClient connection instance

        :return:
            returns a PfsClient Object
        """
        client = PachydermClient(
            self.config["cluster_ip"],
            self.config["port"]
        )
        return client

    def create_repo(self, repo_json):
        """
        creates a new repo on pachyderm cluster
        :param repo_json:
            Information of the repo to be created
        :return:
        """
        if "repo_name" not in repo_json:
            raise RepoNotProvidedException()
        if not self.name_validity_check(repo_json["repo_name"]):
            raise PachydermFieldsNameException()
        description = ""
        if "description" in repo_json:
            description = repo_json["description"]
        self.pachyderm_client.create_new_repo(repo_json["repo_name"],
                                              description)
        return

    def get_repos(self):
        """
        returns the list of all available repos

        :return:
            returns list of repos
        """
        repo_info = self.pachyderm_client.get_repo()
        return self.filter_repo_output(repo_info)

    def delete_repo(self, repo_name):
        """
        deletes a repo from pachyderm cluster

        This is admin level operation
        :param repo_name:
            name of the repo to be deleted
        :return:
            no return statement
        """
        self.pachyderm_client.delete_repo(repo_name)

    def create_branch(self, branch_info):
        """
        creates a new branch in specified repo on pachyderm cluster

        :param branch_info:
            info of the branch to be created
        :return:
        """
        if "repo_name" not in branch_info or "branch_name" not in branch_info:
            raise BranchInfoException("Repo name & branch name is required")

        if not self.name_validity_check(branch_info["branch_name"]) or \
                not self.name_validity_check(branch_info["repo_name"]):
            raise PachydermFieldsNameException()

        self.pachyderm_client.create_new_branch(
            branch_info["repo_name"],
            branch_info["branch_name"]
        )

    def get_branches(self, repo_name):
        """
        returns a list of branches in specified repo

        :param repo_name:
            name of the repo to list the branches
        """
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
        if not self.name_validity_check(repo_name):
            raise PachydermFieldsNameException("repo name is invalid")
        if not self.name_validity_check(branch_name):
            raise PachydermFieldsNameException("branch name is invalid")

        commit_info = self.pachyderm_client.list_commit(repo_name)
        commit_list = []
        for commit_item in commit_info:
            commit_data = self.filter_commit_info(commit_item)
            if commit_data['branch'] == branch_name:
                commit_list.append(commit_data)

        return commit_list

    def push_files(self, files_info):
        """
        pushes file/files into a pachyderm cluster

        :param files_info:
            info of files, repo & branch
        """
        mandatory_fields = ["repo_name", "branch_name", "dataset_name",
                            "path", "description"]
        for field in mandatory_fields:
            if field not in files_info:
                raise DatasetInfoException(f"'{field}' field not provided")
            elif field != "path" and not self.name_validity_check(files_info[field]):
                # path can have '/'. Hence excluded from this check
                raise PachydermFieldsNameException(f"Invalid {field} value")

        if not os.path.exists(files_info["path"]):
            raise DatasetPathException(f"path {files_info['path']} is invalid")
        elif os.path.isfile(files_info["path"]):
            dataset_dir = os.path.dirname(os.path.abspath(files_info["path"]))
        else:
            dataset_dir = files_info["path"]

        # fetches the path of all the files inside the dataset directory
        file_list = self.fetch_file_list(dataset_dir,
                                         files_info["dataset_name"])
        new_commit_id = self.pachyderm_client.push_dataset(
            files_info["repo_name"],
            files_info["branch_name"],
            file_list,
            files_info["description"]
        )

        return new_commit_id

    def manage_xprctl_dataset(self, method, data_info):
        """

        :param method:
            method to be called
        :param data_info:
            info of the dataset
        :return:
            returns appropriate output of list or path
        """
        if "repo_name" not in data_info:
            raise RepoNotProvidedException()
        elif "branch_name" not in data_info and "commit_id" not in data_info:
            raise DatasetInfoException(
                f"Either one of branch name or commit id is required"
            )

        path = "/"
        if "path" in data_info:
            path = data_info["path"]

        branch_name = None
        if "branch_name" in data_info:
            branch_name = data_info["branch_name"]

        commit_id = None
        if "commit_id" in data_info:
            commit_id = data_info["commit_id"]

        if method == "pull":
            return self.pull_dataset(data_info["repo_name"],
                                     branch_name,
                                     path,
                                     commit_id)
        else:
            return self.list_dataset(data_info["repo_name"],
                                     branch_name,
                                     path,
                                     commit_id)

    def push_dataset(self, repo_name, branch_name, dataset, description):
        """
        pushes a dataset into pachyderm cluster

        :param repo_name:
            name of the repo i.e. project in this case
        :param branch_name:
            name of the branch
        :param dataset:
            AbstractDataset object with info on dataset
        :param description:
            brief description regarding this push
        :return:
            returns commit_id if push is successful
        """
        abstract_dataset = datasetmodule.AbstractDataset
        if not isinstance(dataset, abstract_dataset):
            raise DatasetInfoException("Provided dataset is invalid")
        # First Save the dataset locally
        pickle_file = dataset.save()
        dataset_name = dataset.name
        # pickle_file = dataset.get_latest_pickle_file()
        # TODO: Add File, Folder Handling Exceptions
        if not os.path.exists(pickle_file):
            raise Exception
        # checks if pickle_file is a valid file
        if os.path.isfile(pickle_file):
            # if pickle_file path is provided, its directory is fetched
            dataset_dir = os.path.dirname(os.path.abspath(pickle_file))
        else:
            # else it assumes the directory of pickle_file is returned
            dataset_dir = pickle_file

        # fetches the path of all the files inside the dataset directory
        file_list = self.fetch_file_list(dataset_dir, dataset_name)
        new_commit_id = self.pachyderm_client.push_dataset(repo_name,
                                                           branch_name,
                                                           file_list,
                                                           description)

        return new_commit_id

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
        new_dir_path = os.path.join(current_dir, dataset_list["commit"]["id"])
        if len(dataset_list["dataset"]):
            if not os.path.exists(new_dir_path):
                os.makedirs(new_dir_path)

        for file_info in dataset_list["dataset"]:
            if file_info["type"] == "File":
                file_in_bytes = self.pachyderm_client.pull_dataset(
                    repo_name,
                    dataset_list["commit"]["id"],
                    file_info["path"]
                )
                # This needs to be done because path in file_info contains pachyderm path
                # It always starts with / . Hence it needs to be excluded exclusively
                file_info["path"] = file_info["path"].lstrip("/")
                local_write_path = os.path.join(new_dir_path, file_info["path"])
                self.write_to_file(path=local_write_path, content=file_in_bytes)
        return new_dir_path

    def list_dataset(self, repo_name, branch_name, path, commit_id=None):
        """
        list of dataset as per provided information

        :param repo_name:
            name of the repo
        :param branch_name:
            name of the branch
        :param path:
            (Optional) path of the dir in which dataset might be present
                       path of the dataset
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
            data_info["repo_name"], data_info["commit_id"], data_info["path"]
        )
        list_output["dataset"] = dataset_list

        commit = self.pachyderm_client.inspect_commit(
            data_info["repo_name"], data_info["commit_id"])
        list_output["commit"] = self.filter_commit_info(commit)
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
        data_info["repo_name"] = repo_name

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

            data_info["commit_id"] = commit_id
            data_info["branch_name"] = commit_info.branch.name
        else:
            branch_info = self.pachyderm_client.inspect_branch(
                repo_name,
                branch_name
            )
            data_info["branch_name"] = branch_name
            data_info["commit_id"] = branch_info.head.id

        data_info["path"] = "/"
        if path:
            data_info["path"] = path

        return data_info

    @staticmethod
    def fetch_file_list(dataset_dir, dataset_name):
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
                destination_path = file_path.replace(dataset_dir, pachyderm_destination_path, 1)
                file_list.append((file_path, destination_path))

        return file_list

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
            temp_repo["name"] = repo_item.repo.name
            temp_repo["Date of creation"] = self.find_date_from_seconds(repo_item.created.seconds)
            repo_list.append(temp_repo)
        return repo_list

    @staticmethod
    def find_date_from_seconds(number_of_seconds):
        """
        Takes number of seconds as input and outputs date from 1970/1/1

        :param number_of_seconds:
            number of seconds
        :return:
            date as string
        """
        start_date = datetime.datetime(1970, 1, 1)
        new_date_object = start_date + datetime.timedelta(seconds=number_of_seconds)
        new_date = new_date_object.strftime("%d/%m/%y")
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

    @staticmethod
    def write_to_file(path, content):
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
            if file_extension == ".pkl":
                # if it is pickle file we use pickle to dump it onto file
                dataset_object = pickle.loads(content)
                pickle.dump(dataset_object, out_file, pickle.HIGHEST_PROTOCOL)
            else:
                out_file.write(content)
        return

    @staticmethod
    def name_validity_check(name):
        """
        Checks if the name provided contains only alphanumeric characters,
        underscore or dashes

        :param name:
            (str) : name
        :return:
            check status i.e. True or False
        """
        accepted_pattern = r"[\w, -]+$"
        if not isinstance(name, str):
            raise PachydermFieldsNameException()
        match = re.match(accepted_pattern, name)
        if not match:
            return False
        return True
