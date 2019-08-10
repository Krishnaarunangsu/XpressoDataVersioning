""" Class design for Dataset"""

from xpresso.ai.core.data.dataset import DatasetType

__all__ = ['SemiStructuredDataset']
__author__ = 'Srijan Sharma'


class SemiStructuredDataset:
    """ SemiStructuredDataset stores the data in a plain file format
    """

    def __init__(self, dataset_name: str = "default",
                 description: str = "This is a semi structured dataset"):
        super().__init__(dataset_name=dataset_name,
                         description=description)

        self.type = DatasetType.STRUCTURED

    def import_dataset(self, data_source, local_storage_required: bool = False,
                       sample_percentage: float = 100):
        """ Fetches dataset from multiple data sources and loads them
        into a dataset"""
        pass

    def save(self):
        pass

    def load(self):
        pass

    def diff(self):
        pass