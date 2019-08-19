""" Class design for Dataset"""
import copy
import datetime
from abc import abstractmethod

import pandas as pd

"""
from xpresso.ai.admin.controller.persistence.persistentce_connection import \
    create_persistence_object
from xpresso.ai.admin.controller.user_management.usermanager import UserManager
from xpresso.ai.admin.controller.utils.xpr_exceptions import \
    AuthenticationFailedException
    """
from xpresso.ai.core.data.dataset_info import DatasetInfo
from xpresso.ai.core.data.dataset_type import DatasetType
#from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser

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
        #self.config = XprConfigParser(config_file_path=config_path)
        self.config = ""

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



    def import_from_dataset(self, dataset):
        """
        Import properties of dataset from another dataset

        Args:
            dataset: source dataset. Properties of these dataset will be
                     updated in the current dataset
        """
        self.__dict__ = copy.deepcopy(dataset.__dict__)
