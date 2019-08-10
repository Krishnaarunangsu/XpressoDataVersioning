""" Class design for Dataset"""
import copy
import pickle
from abc import abstractmethod
import datetime
import pandas as pd
import os

# from xpresso.ai.admin.controller.client.controller_client import \
#     ControllerClient
import xpresso.ai.admin.controller.client.controller_client as controller

from utils.xpr_exceptions import \
    SerializationFailedException, DeserializationFailedException
from xpresso.ai.admin.controller.persistence.persistentce_connection import \
    create_persistence_object
from xpresso.ai.admin.controller.user_management.usermanager import UserManager
from utils.xpr_exceptions import AuthenticationFailedException
from data_exploration.dataset_info import DatasetInfo
from data_exploration.dataset_type import DatasetType
#from xpresso.ai.core.logging.xpr_log import XprLogger
from utils.xpr_config_parser import XprConfigParser

__all__ = ['AbstractDataset']
__author__ = 'Srijan Sharma'

#logger = XprLogger()


class AbstractDataset(object):
    """ Dataset is an abstract storage class. It is responsible for complete
    lifecycle of the a dataset. It start with importing from the source,
    saving/loading from the local, performing exploration and analysis
    over the data
    """

    def __init__(self, dataset_name: str = "default",
                 description: str = "This is a dataset",
                 config_path: str = XprConfigParser.DEFAULT_CONFIG_PATH):
        self.config = XprConfigParser(config_file_path=config_path)

        self.data = pd.DataFrame()
        self.name = dataset_name
        self.type = DatasetType.STRUCTURED
        self.description = description
        self.num_records = len(self.data)
        self.creation_date = datetime.datetime.now()
        self.creation_by = "default"
        self.project = "default"
        self.repo = "default"
        self.branch = "master"
        self.version = 1
        self.tag = "1.0.0"
        self.info = DatasetInfo()
        self.local_storage_required = False
        self.sample_percentage = 100.00

    @abstractmethod
    def import_dataset(self, data_source, local_storage_required: bool = False,
                       sample_percentage: float = 100):
        """
        Fetches dataset from multiple data sources and loads them
        into a dataset

        Args:
            data_source(str): string path or uri of the data source
            local_storage_required(bool):
            sample_percentage(sbool):
        """

    @abstractmethod
    def save(self):
        """ Serialize the dataset and store it into a local storage"""

    @abstractmethod
    def load(self):
        """ Load the data set from local storage and deserialize to update
        the dataset """

    @abstractmethod
    def diff(self):
        """ Find the diff between two dataset"""

    def serialize(self) -> bytes:
        """ Serialize the dataset object into a byte order string """
        try:
            serialized_byte = pickle.dumps(self,
                                           protocol=pickle.HIGHEST_PROTOCOL)
            return serialized_byte
        except (pickle.PickleError, pickle.PicklingError):
            logger.error("Failed to serialize the dataset object")
            raise SerializationFailedException("Serialization failed")

    def deserialize(self, serialized_byte: bytes, update_self=True) -> object:
        """
        Deserialize the dataset object from byte order string into the
        dataset object

        Args:
            serialized_byte: byte ordered string of a serialized dataset object
            update_self: If set to True, update the properties of self object

        Returns:
            dataset object
        """
        try:
            deserialize_obj = pickle.loads(serialized_byte)
            if update_self:
                self.import_from_dataset(deserialize_obj)
            return deserialize_obj
        except (pickle.PickleError, pickle.PicklingError):
            logger.error("Failed to  deserialize the byte string")
            raise DeserializationFailedException("Deserialization failed")

    def get_local_storage_path(self):
        """ Returns the path of the local storage"""

        client = controller.ControllerClient()
        token = client.get_token()
        persistence_manager = create_persistence_object(self.config)
        user_id = UserManager(persistence_manager).get_users({"token": token})
        if len(user_id) == 0:
            raise AuthenticationFailedException("User ID not found for token")
        print(user_id)
        local_storage = os.path.join(user_id[0]['uid'], self.project,
                                     "datasets",
                                     self.name)
        os.makedirs(local_storage, exist_ok=True)
        return local_storage

    @staticmethod
    def get_pickle_file_path(pickle_base_name_pattern, number=1):
        """
        Get the pickle file path
        Args:
            pickle_base_name_pattern: pickle file name pattern, it may be a
                                      prefix/suffix to categorize the pickle
                                      file accurately
            number: Specify which version the pickle file. default=1

        Returns:
            str: absolute pickle file path
        """
        return pickle_base_name_pattern % '{0:0>5}'.format(number)

    def get_pickle_pattern(self):
        """ Generates a name pattern for all the pickle file. This is
         used to generate the absolute file path maintaining the versions"""
        parent_dir = self.get_local_storage_path()
        return os.path.join(parent_dir, f"{self.name}_dataset__%s.pkl")

    def get_highest_pickle_file_number(self, pickle_base_name_pattern):
        """
        Get the highest version of the pickle files available
        Args:
            pickle_base_name_pattern: pickle file name pattern, it may be a
                                      prefix/suffix to categorize the pickle
                                      file accurately

        Returns:
            int: highest version number available
        """
        # Finding the next numbered filename
        current_highest = 1
        while os.path.exists(self.get_pickle_file_path(
            pickle_base_name_pattern, current_highest)):
            current_highest *= 2
        current_highest /= 2
        while os.path.exists(
            self.get_pickle_file_path(pickle_base_name_pattern,
                                      current_highest)):
            current_highest += 1
        return current_highest - 1

    def get_latest_pickle_file_name(self):
        """ Get latest pickle file name"""
        pickle_base_name_pattern = self.get_pickle_pattern()
        current_highest = self.get_highest_pickle_file_number(
            pickle_base_name_pattern)
        return self.get_pickle_file_path(pickle_base_name_pattern,
                                         current_highest)

    def get_next_pickle_file_name(self):
        """ Get next pickle file name in terms of counter"""
        pickle_base_name_pattern = self.get_pickle_pattern()
        current_highest = self.get_highest_pickle_file_number(
            pickle_base_name_pattern)
        return self.get_pickle_file_path(pickle_base_name_pattern,
                                         current_highest + 1)

    def import_from_dataset(self, dataset):
        """
        Import properties of dataset from another dataset

        Args:
            dataset: source dataset. Properties of these dataset will be
                     updated in the current dataset
        """
        self.__dict__ = copy.deepcopy(dataset.__dict__)
