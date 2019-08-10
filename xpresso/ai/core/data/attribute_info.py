__all__ = ["AttributeInfo"]
__author__ = "Srijan Sharma"

from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.data.dataset_type import DECIMAL_PRECISION
import pandas as pd
import numpy as np
from enum import Enum


class DataType(Enum):
    """
    Enum class to standardize all the datatype
    """
    ORDINAL = "ordinal"
    NOMINAL = "nominal"
    NUMERIC = "numeric"
    STRING = "string"
    TEXT = "text"
    DATE = "date"

    FLOAT = "float"
    INT = "int"
    OBJECT = "object"
    BOOL = "bool"
    PD_DATETIME = "datetime"

    def __str__(self):
        return self.value


class AttributeInfo:

    def __init__(self, attribute_name):
        self.logger = XprLogger()
        self.name = attribute_name
        self.metrics = dict()

    def populate(self, data):

        na_count, na_count_percentage, missing_count, missing_count_percentage = self.na_analysis(
            data)

        self.metrics["na_count"] = na_count
        self.metrics["na_count_percentage"] = na_count_percentage
        self.metrics["missing_count"] = missing_count
        self.metrics["missing_count_percentage"] = missing_count_percentage

        if str(self.type) is "numeric":
            min, max, mean, std, quartiles, deciles, outliers, pdf, \
            kurtosis = self.numeric_analysis(data)

            self.metrics["min"] = min
            self.metrics["max"] = max
            self.metrics["mean"] = mean
            self.metrics["std"] = std
            self.metrics["quartiles"] = quartiles
            self.metrics["deciles"] = deciles
            self.metrics["outliers"] = outliers
            self.metrics["pdf"] = pdf
            self.metrics["kurtosis"] = kurtosis

        elif str(self.type) is "ordinal" or self.type is "nominal":
            outliers, freq_count = self.categorical_analysis(data)
            self.metrics["outliers"] = outliers
            self.metrics["freq_count"] = freq_count

        elif str(self.type) is "string":
            self.metrics["tobedone"] = "Exploration of string type data"

        elif str(self.type) is "text":
            self.metrics["tobedone"] = "Exploration of text type data"

        elif str(self.type) is "date":
            min_date, max_date, day_count, month_count, year_count = \
                self.date_analysis(data)
            self.metrics["min"] = min_date
            self.metrics["max"] = max_date
            self.metrics["day_count"] = day_count
            self.metrics["month_count"] = month_count
            self.metrics["year_count"] = year_count

    @staticmethod
    def na_analysis(data):

        num_rows = float(data.size)
        na_count = float(data.isna().sum())

        na_count_percentage = round((na_count / num_rows) * 100,
                                    DECIMAL_PRECISION)
        missing_count = float((data == "").sum())
        missing_count_percentage = round((missing_count / num_rows) * 100,
                                         DECIMAL_PRECISION)

        return na_count, na_count_percentage, missing_count, missing_count_percentage

    @staticmethod
    def numeric_analysis(data, outlier_margin=10,
                         probability_dist_bins=100):

        summary = data.describe().round(DECIMAL_PRECISION)

        min = summary["min"]
        max = summary["max"]
        mean = summary["mean"]
        std = summary["std"]

        quartiles = data.quantile([.0, .25, .5, .75,1]).round(
            DECIMAL_PRECISION).values

        try:
            deciles = pd.qcut(data, 10, retbins=True)[1].round(
                DECIMAL_PRECISION)
        except ValueError as e:
            print("Dropping duplicates for calculating deciles for {}"
                  .format(data.name))
            deciles = pd.qcut(data, 10, duplicates='drop', retbins=True)[
                1].round(DECIMAL_PRECISION)
        outliers = round(data[(np.abs(data - mean) > (
                outlier_margin * std))],
                         DECIMAL_PRECISION).to_numpy()

        pdf = pd.cut(data, probability_dist_bins).value_counts()
        pdf.index = pdf.index.astype(str)
        pdf = pdf.to_dict()
        kurtosis = round(data.kurtosis(), DECIMAL_PRECISION)
        return min, max, mean, std, quartiles, deciles, outliers, pdf, \
               kurtosis

    @staticmethod
    def categorical_analysis(data, threshold=2):
        '''
        :param data: Input pandas categorical series data
        :param threshold: Count percentage value for defining Outlier Categories
        :return:
        '''
        num_rows = float(data.size)
        freq_count = data.value_counts().to_dict()
        outliers = list()
        for label in freq_count.keys():
            if (freq_count[label] / num_rows) * 100 < threshold:
                outliers.append(label)

        return np.array(outliers), freq_count

    @staticmethod
    def date_analysis(data):
        min = data.min()
        max = data.max()

        date_df = pd.DataFrame({"day": data.dt.day_name(),
                                "month": data.dt.month_name(),
                                "year": data.dt.year.values})

        day_count = date_df["day"].dropna().value_counts().to_dict()
        month_count = date_df["month"].dropna().value_counts().to_dict()
        year_count = date_df["year"].dropna().astype("int").value_counts(
        ).to_dict()

        return min, max, day_count, month_count, year_count
