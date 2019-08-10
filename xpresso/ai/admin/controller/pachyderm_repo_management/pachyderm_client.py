import python_pachyderm as pachyderm
import os
import pickle

from grpc._channel import _Rendezvous as PachClientException
from xpresso.ai.admin.controller.exceptions.xpr_exceptions \
    import PachydermOperationException

# TODO: Make sure to add exceptions for all the client methods


class PachydermClient:
    """
    Our own wrapper for pachyderm python client


    """
    def __init__(self, host, port, auth_token=None):
        """

        :param host:
        :param port:
        :param auth_token:
        """
        self.host = host
        self.port = port
        self.auth_token = auth_token
        self.client = self.connect(host, port)

    @staticmethod
    def connect(host, port):
        """

        :param host:
        :param port:
        :return:
        """
        client = pachyderm.PfsClient(host, port)
        return client

    def create_new_repo(self, repo_name, description=None, update=None):
        """
        :param repo_name:
            name of the repo
        :param description:
            description on the repo(Optional)
        :param update:
            update flag to overwrite if repo already exists
        :return:
        """
        self.client.create_repo(repo_name, description, update)

    def get_repo(self):
        """
        returns the list of all the repos on pachyderm cluster

        :return:
            list of repos
        """
        repos = self.client.list_repo()
        return repos

    def delete_repo(self, repo_name):
        """
        deletes a repo
        :param repo_name:
            name of the repo to be deleted
        :return:
        """
        self.client.delete_repo(repo_name)

    def create_new_branch(self, repo_name, branch_name):
        """
        create a new branch in specified repo

        :param repo_name:
            name of the repo
        :param branch_name:
            name of the branch
        :return:
        """
        try:
            self.client.create_branch(repo_name, branch_name)
        except PachClientException as err:
            raise PachydermOperationException(err.details())

    def get_branch(self, repo_name):
        """
        returns list of branches in a repo
        :param repo_name:
            name of the repo
        """
        branches = self.client.list_branch(repo_name)
        return branches

    def inspect_branch(self, repo_name, branch_name):
        """
        provides information on a branch

        :param repo_name:
            name of the repo where branch resides
        :param branch_name:
            name of the branch
        :return:
            branch information object
        """
        try:
            branch_info = self.client.inspect_branch(repo_name, branch_name)
            return branch_info
        except PachClientException as err:
            raise PachydermOperationException(err.details())

    def delete_branch(self, repo_name, branch_name):
        """
        deletes a branch from the specified repo

        :param repo_name:
            name of the repo branch is in
        :param branch_name:
            name of the branch that needs to be deleted
        """
        self.client.delete_branch(repo_name, branch_name)

    def inspect_commit(self, repo_name, commit_id, block_state=None):
        """
        provides information on a commit

        :param repo_name:
            name of the repo
        :param commit_id:
            commit id
        :param block_state:
        :return:
            returns CommitInfo Object
        """
        try:
            commit_info = self.client.inspect_commit((repo_name, commit_id), block_state)
            return commit_info
        except PachClientException as err:
            raise PachydermOperationException(err.details())

    def list_commit(self, repo_name, upper_commit=None, lower_commit=None, count=None):
        """
        lists commits in a repo

        :param repo_name:
            name of the commit
        :param upper_commit:
            id of the last commit that needs to be shown
        :param lower_commit:
            id of the commit from which list starts
        :param count:
            number of commits to be shown
        :return:
            A in-flight _Rendezvous object
        """
        if upper_commit:
            upper_commit = (repo_name, upper_commit)
        if lower_commit:
            lower_commit = (repo_name, lower_commit)
        commits = self.client.list_commit(repo_name, upper_commit, lower_commit, count)
        if commits.done():
            raise PachydermOperationException(commits.details())
        return commits

    def delete_commit(self, repo_name, commit_id):
        """
        deletes a commit and its contents

        :param repo_name:
            name of the repo
        :param commit_id:
            id of the commit that needs to be deleted
        """
        try:
            self.client.delete_commit((repo_name, commit_id))
        except PachClientException as err:
            raise PachydermOperationException(err.details())

    @staticmethod
    def fetch_dataset_info(file_object):
        """
        Fetches filename & path from FileInfo object

        :param file_object:
            FileInfo object containing info for a file
        :return:
            returns a dict with required file info
        """
        info = {}
        path = file_object.file.path
        info["path"] = path
        # finds the index of / from right end
        # If not found i.e. path itself is file_name it returns -1
        # name start index will be right after last '/' or 0
        name_start_index = path.rfind("/") + 1
        info["file_name"] = path[name_start_index:]
        info["type"] = "File" if file_object.file_type == 1 else "Folder"
        info["size_in_bytes"] = file_object.size_bytes
        return info

    def list_dataset(self, repo_name, commit_id, path="/", history=None, include_contents=None):
        """
        list the dataset in a branch or provided commit

        :param repo_name:
            name of the repo
        :param commit_id:
            id of the commit
        :param path:
            path to the dataset file/folder
        :param history:
            (Optional) retrieves previous versions of file
        :param include_contents:
            includes file contents in response
        :return:

        """
        files = self.client.list_file((repo_name, commit_id), path, history, include_contents)
        if files.done():
            raise PachydermOperationException(files.details())
        file_list = []
        sub_dir_path_list = []
        for file_item in files:
            file_info = self.fetch_dataset_info(file_item)
            file_list.append(file_info)
            # if file type is a directory
            if file_item.file_type == 2:
                sub_dir_path_list.append(file_item.file.path)

        for sub_dir_path in sub_dir_path_list:
            file_list += self.list_dataset(repo_name, commit_id, sub_dir_path,
                                           history, include_contents)

        return file_list

    def push_dataset(self, repo_name, branch_name, file_path_list, push_description=None):
        """
        pushes a dataset into pachyderm cluster

        starts a new commit and pushes the file provided at the `path`
        to the cluster

        :param repo_name:
            name of the repo
        :param branch_name:
            name of the branch
        :param file_path_list:
            list of paths of all the files
        :param push_description:
            description for this push
        :return:
            status of the push
        """
        # TODO: add FileNotFound & byte encoding exceptions
        try:
            # opens a new commit
            new_commit = self.client.start_commit(repo_name, branch_name, parent=None,
                                                  description=push_description)
        except PachClientException as err:
            # returns in case of opening a new commit
            raise PachydermOperationException(err.details())

        try:
            for (local_path, pachyderm_path) in file_path_list:
                # fetching file extension before reading
                file_extension = os.path.splitext(local_path)[1]

                with open(local_path, "rb") as dataset:
                    if file_extension == ".pkl":
                        # if pickle file is provided
                        dataset_object = pickle.load(dataset)
                        byte_data = pickle.dumps(dataset_object)
                    else:
                        byte_data = dataset.read()
                        # byte_data = data.encode('utf-8')
                self.client.put_file_bytes((repo_name, new_commit.id), pachyderm_path, byte_data)

            self.client.finish_commit((repo_name, new_commit.id))
            return new_commit.id
        except PachClientException as err:
            # removes the above commit
            self.client.delete_commit((repo_name, new_commit))
            raise PachydermOperationException(err.details())
        except UnicodeError as err:
            raise PachydermOperationException("File encoding failure")

    def pull_dataset(self, repo_name, commit_id, path):
        """
        Pulls dataset/file at the specified path of the commit

        :param repo_name:
            name of the repo where the dataset is saved
        :param commit_id:
            id of the commit when the dataset is pushed
        :param path:
            path of the file in the pachyderm cluster
        :return:
            returns the fileInfo object if success
        """
        commit_tuple = (repo_name, commit_id)
        files = self.client.get_file(commit_tuple, path)
        try:
            for file in files:
                return file
        except PachClientException as err:
            raise PachydermOperationException(err.details())

    def delete_dataset(self, repo_name, commit_id, file_path):
        """
        deletes a dataset from the cluster

        :param repo_name:
            name of the repo dataset is in
        :param commit_id:
            id of the commit to find the dataset
        :param file_path:
            path of the dataset on the pachyderm cluster
        """
        try:
            if not file_path or file_path == "/":
                self.delete_commit(repo_name, commit_id)
            else:
                self.client.delete_file((repo_name, commit_id), file_path)
        except PachClientException as err:
            raise PachydermOperationException(err.details())
