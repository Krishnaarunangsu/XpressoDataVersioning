""" Class design for Dataset"""

from xpresso.ai.core.data.dataset import AbstractDataset
from xpresso.ai.core.data.dataset_type import DatasetType

__all__ = ['UnstructuredDataset']
__author__ = 'Srijan Sharma'


class UnstructuredDataset(AbstractDataset):
    """ UnstructuredDataset stores the data in a plain file format
    """

    def __init__(self, dataset_name: str = "default",
                 description: str = "This is a unstructured dataset"):
        super().__init__(dataset_name=dataset_name,
                         description=description)

        self.type = DatasetType.STRUCTURED

    def import_dataset(self, data_source, local_storage_required: bool = False,
                       sample_percentage: float = 100):
        """ Fetches dataset from multiple data sources and loads them
        into a dataset"""
        pass

    def save(self):
        """ Save the dataset into the local file system in
        a serialized format"""
        serialized_data = self.serialize()
        new_pickle_file_name = self.get_next_pickle_file_name()
        with open(new_pickle_file_name, "wb") as pickle_fs:
            pickle_fs.write(serialized_data)
        return new_pickle_file_name

    def load(self, pickle_file_name=None):
        """ Load the dataset from the local file system
        in a serialized format"""
        if not pickle_file_name:
            pickle_file_name = self.get_latest_pickle_file_name()
        with open(pickle_file_name, "rb") as pickle_fs:
            serialized_data = pickle_fs.read()
            dataset_obj = self.deserialize(serialized_data)
            self.import_from_dataset(dataset_obj)
            return True

    def diff(self):
        pass

