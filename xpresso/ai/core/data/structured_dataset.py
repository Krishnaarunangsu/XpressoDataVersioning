""" Class design for Dataset"""

import pickle
import pandas as pd

from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    SerializationFailedException, DeserializationFailedException
from xpresso.ai.core.data.dataset import AbstractDataset
from xpresso.ai.core.data.dataset_explorer import Explorer
from xpresso.ai.core.data.dataset_type import DatasetType
#from xpresso.ai.core.logging.xpr_log import XprLogger
import csvdiff

__all__ = ['StructuredDataset']
__author__ = 'Srijan Sharma'


# This is indented as logger can not be serialized and can not be part
# of dataset
#logger = XprLogger()


class StructuredDataset(AbstractDataset):
    """ StructuredDataset stores the data in tabular format. It reads data
    from csv, excel or any database. It stores the dataset into local storage
    in pickle format."""

    def __init__(self, dataset_name: str = "default",
                 description: str = "This is a structured dataset"):
        super().__init__(dataset_name=dataset_name,
                         description=description)

        self.type = DatasetType.STRUCTURED

    def import_dataset(self, data_source, local_storage_required: bool = False,
                       sample_percentage: float = 100):
        """ Fetches dataset from multiple data sources and loads them
        into a dataset"""
        self.data = pd.read_csv(data_source)
        self.local_storage_required = local_storage_required
        print(self.local_storage_required)
        self.sample_percentage = sample_percentage

    def save(self):
        """ Save the dataset into the local file system in
        a serialized format

        Returns:
            str: folder path where serialized data has been stored
        """
        serialized_data = self.serialize()
        new_pickle_file_name = self.get_next_pickle_file_name()
        with open(new_pickle_file_name, "wb") as pickle_fs:
            pickle_fs.write(serialized_data)
        return new_pickle_file_name

    def load(self, pickle_file_name=None):
        """
        Load the dataset from the local file system
        in a serialized format

        Args:
            pickle_file_name: name of the exact folder where pickles are present.
                              if not, it will pick from default directory

        Returns:
            bool: True if load is successful, False otherwise.
        """
        if not pickle_file_name:
            pickle_file_name = self.get_latest_pickle_file_name()
        with open(pickle_file_name, "rb") as pickle_fs:
            serialized_data = pickle_fs.read()
            dataset_obj = self.deserialize(serialized_data)
            self.import_from_dataset(dataset_obj)
            return True

    def diff(self,second):
        """ Finds the difference between two dataset class"""
        metadata_diff = self.compare_metadata(self.info.attributeInfo,
                                              second.info.attributeInfo)
        data_diff = self.compare_data(self.data,second.data)
        print(metadata_diff)
        print(data_diff)
        pass


    @staticmethod
    def compare_metadata(latest,old):
        """
        Compares the metadata of two dataset classes i.e. attributeInfo for
        each dataset is compared
        """
        identical = True
        metadata_old = list()
        metadata_latest = list()
        difference = list()

        for attr in latest:
            metadata_latest.append((attr.name,attr.dtype,attr.type))

        for attr in old:
            metadata_old.append((attr.name,attr.dtype,attr.type))

        metadata_diff = list(set(metadata_old).symmetric_difference(set(
            metadata_latest)))

        for attr_diff in metadata_diff:

            if attr_diff  in metadata_old:
                name = attr_diff[0]
                type = attr_diff[2]

                #If the name is present in the old attributeinfo, but not in
                # the new one
                if name not in [attr[0] for attr in metadata_latest]:
                    print("{} has been removed.Not found in the latest "
                          "version".format(attr_diff))
                    difference.append((name,"removed"))
                    identical = False

                #if the attribute corresponding to that name is present in
                # old and new attributeinfo, but only the type has changed
                for attr_latest in metadata_latest:
                    if name is attr_latest[0] and type is not attr_latest[2]:
                        print("Type of {} changed from {} to {} in the "
                              "latest version".format(
                            name,type,attr_latest[2]))
                        difference.append((name,"updated"))
                        identical = False
                        break

            #if the attribute is present in latest version but not in the old
            # version
            elif attr_diff in metadata_latest:
                name = attr_diff[0]
                print("{} added in the latest version".format(attr_diff))
                difference.append((name,"added"))
                identical = False

        if identical:
            print("Metadata for both versions identical")
        return difference


    @staticmethod
    def compare_data(latest,old):
        """
        Compares the pandas dataframe of two dataset classes
        """
        latest =  latest.dropna()
        old = old.dropna()

        latest['id'] = latest.apply(lambda x: hash(tuple(x)), axis = 1)
        old['id'] = old.apply(lambda x: hash(tuple(x)), axis=1)
        old_records = old.to_dict("records")
        latest_records = latest.to_dict("records")
        data_diff = csvdiff.diff_records(old_records,latest_records, ['id'])
        return data_diff

if __name__ == "__main__":
    dataset = StructuredDataset()
    dataset.import_dataset("./config/test/data/test.csv")
    explorer = Explorer(dataset)
    explorer.understand()
    explorer.explore_attributes()

    dataset2 = StructuredDataset()
    dataset2.import_dataset("./config/test/data/test.csv")
    explorer = Explorer(dataset2)
    explorer.understand()
    explorer.explore_attributes()

    dataset.diff(dataset2)

    for val in dataset.info.attributeInfo:
        if "ordinal" in val.name.lower():
            val.type = "ordinal"

    explorer.explore_metrics()
    for val in dataset.info.attributeInfo:
        print("Name : {} , Dtype : {} ,  Type : {} , Metrics : {} \n".format(
            val.name, val.dtype, val.type, val.metrics))
