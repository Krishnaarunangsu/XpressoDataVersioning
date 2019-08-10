__all__ = ['Explorer']
__author__ = 'Srijan Sharma'


from xpresso.ai.core.data.dataset_info import DatasetInfo


class Explorer:

    def __init__(self, dataset):
        self.dataset = dataset

    def understand(self):
        self.dataset.info.understand_attributes(self.dataset.data, self.dataset.type)

    def explore_attributes(self):
        self.dataset.info.populate_attribute(self.dataset.data, self.dataset.type)

    def explore_metrics(self):
        self.dataset.info.populate_metric(self.dataset.data, self.dataset.type)
