from etl.reader_factory import get_data_source
from etl.spark_initializer import SparkInitializer

class Extractor:
    def __init__(self):
        self.spark = SparkInitializer.get_spark()

    def extract(self):
        raise ValueError('Not implemented')


class DataFrameExtractor(Extractor):
    def extract(self, data_type, path):
        inputDF = get_data_source(data_type, path)
        return inputDF
