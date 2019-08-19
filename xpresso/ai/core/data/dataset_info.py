""" Class definition of dataset Info """

import itertools

import pandas as pd
import scipy.stats as stats
from dateutil.parser import parse

# from xpresso.ai.admin.controller.exceptions.xpr_exceptions import *
from xpresso.ai.core.data.attribute_info import AttributeInfo, DataType
from xpresso.ai.core.data.dataset_type import DatasetType, DECIMAL_PRECISION
#from xpresso.ai.core.logging.xpr_log import XprLogger

__all__ = ['DatasetInfo']
__author__ = 'Srijan Sharma'

# This is indented as logger can not be serialized and can not be part
# of dataset
#logger = XprLogger()

from xpresso.ai.core.data.exception_handling.custom_exception import InvalidDataTypeException
class DatasetInfo:
    """ DatasetInfo contains the detailed information about the
    dataset. This information contains the attribute list,
    attribute type, attribute metrics"""

    def __init__(self):
        self.attributeInfo = list()
        self.metrics = dict()
        return

    def understand_attributes(self, data, dataset_type: DatasetType):
        if not isinstance(dataset_type, DatasetType):
            #logger.error("Unacceptable Data type provided. Type {} is "
                         #"not supported".format(dataset_type))
            raise InvalidDataTypeException("Provided Data Type : {} not "
                                           "supported".format(dataset_type))

        # For structured datatype
        elif dataset_type == DatasetType.STRUCTURED:
            self.attributeInfo = list(map(lambda x: AttributeInfo(x),
                                          data.columns))
            for attr in self.attributeInfo:
                attr.dtype = data[attr.name].dtype
                attr.type = self.find_attr_type(data[attr.name], attr.dtype)
                if attr.type is DataType.DATE.value:
                    data[attr.name] = data[attr.name].apply(
                        lambda x: x if pd.isna(x) else parse(x))
                    attr.dtype = data[attr.name].dtype

        # For semi-structured data type
        elif dataset_type == DatasetType.SEMI_STRUCTURED:
            self.attributeInfo = list()

        # For unstructured data type
        elif dataset_type == DatasetType.UNSTRUCTURED:
            self.attributeInfo = list()

    @staticmethod
    def is_date(date_values):
        is_date_bool = list()
        for date in date_values.iteritems():
            try:
                parse(date[1])
                is_date_bool.append(True)
            except ValueError:
                is_date_bool.append(False)
        ret = True if sum(is_date_bool) > len(is_date_bool) / 2 else False
        return ret

    # Below method finds the data type of the attributes
    @staticmethod
    def find_attr_type(data, dtype, threshold=5, length_threshold=50):
        num_rows = float(data.size)
        unique_values = data.unique().tolist()
        unique_proportion = (float(len(unique_values)) / num_rows) * 100
        dtype = str(dtype)

        data_type = DataType.STRING.value
        if DataType.FLOAT.value in dtype or DataType.INT.value in dtype:
            if unique_proportion < threshold:
                data_type = DataType.NOMINAL.value

            else:
                data_type = DataType.NUMERIC.value

        elif DataType.OBJECT.value in dtype:
            max_length = data[~data.isna()].map(len).max()
            if DatasetInfo.is_date(data[~data.isna()][0:10]):
                data_type = DataType.DATE.value

            elif unique_proportion < threshold:
                data_type = DataType.NOMINAL.value

            elif max_length < length_threshold:
                data_type = DataType.STRING.value
            else:
                data_type = DataType.TEXT.value

        elif DataType.BOOL.value in dtype:
            data_type = DataType.NOMINAL.value

        return data_type

    def populate_attribute(self, data, date_type):
        if not isinstance(date_type, DatasetType):
            #logger.error("Unacceptable Data type provided. Type {} is "
                         #"not supported".format(date_type))
            raise InvalidDataTypeException("Provided Data Type : {} not "
                                           "supported".format(date_type))

        # For structured datatype
        elif date_type == DatasetType.STRUCTURED:
            for attr in self.attributeInfo:
                attr.populate(data[attr.name])

        # For semi-structured data type
        elif date_type == DatasetType.SEMI_STRUCTURED:
            print("Populate method for semi structured")

        # For unstructured data type
        elif date_type == DatasetType.UNSTRUCTURED:
            print("Populate method for unstructured")

    # populates multi variate metric analysis
    def populate_metric(self, data, data_type):
        if not isinstance(data_type, DatasetType):
            #logger.error("Unacceptable Data type provided. Type {} is "
                         #"not supported".format(data_type))
            raise InvalidDataTypeException("Provided Data Type : {} not "
                                           "supported".format(data_type))
        # For structured datatype
        elif data_type == DatasetType.STRUCTURED:
            data_copy = data
            self.metrics["num_records"] = len(data_copy)
            numeric_field = list()
            nominal_field = list()
            ordinal_field = list()

            for val in self.attributeInfo:
                if val.type is DataType.NUMERIC.value:
                    numeric_field.append(val.name)
                elif val.type is DataType.NOMINAL.value:
                    nominal_field.append(val.name)
                elif val.type is DataType.ORDINAL.value:
                    ordinal_field.append(val.name)

            self.metrics["pearson"] = data[numeric_field].corr(
                method="pearson").round(DECIMAL_PRECISION).unstack().to_dict()

            self.metrics["spearman"] = data[numeric_field
                                            + ordinal_field].corr(
                method="spearman").round(DECIMAL_PRECISION).unstack().to_dict()

            nominal_nominal_comb = list(
                itertools.product(nominal_field, nominal_field))
            nominal_ordinal_comb = list(itertools.product(nominal_field,
                                                          ordinal_field))
            chisquare_combination = nominal_nominal_comb + nominal_ordinal_comb

            self.metrics["chi_square"] = dict()
            for val in chisquare_combination:
                self.metrics["chi_square"][val] = self.ChiSquareTest(data,
                                                                     val[0],
                                                                     val[1])

        # For semi-structured data type
        elif data_type == DatasetType.SEMI_STRUCTURED:
            print("Multivariate analysis or semi structured")

        # For unstructured data type
        elif data_type == DatasetType.UNSTRUCTURED:
            print("Multivariate analysis for unstructured")

    @staticmethod
    def ChiSquareTest(df, x, y):
        x = df[x].astype(str)
        y = df[y].astype(str)

        dfObserved = pd.crosstab(y, x)
        chi2, p, dof, expected = stats.chi2_contingency(dfObserved.values)
        p = p
        chi2 = chi2
        dof = dof
        return (round(p, DECIMAL_PRECISION))
