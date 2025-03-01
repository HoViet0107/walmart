from etl.reader_factory import get_data_source


class Extractor:
    def __init__(self, spark):
        self.spark = spark

    def extract(self):
        print('1')
        raise ValueError('Not implemented')


class DataFrameExtractor(Extractor):
    def __init__(self, spark):
        super().__init__(spark)

    def extract(self, data_type, path):
        inputDF = get_data_source(self.spark, data_type, path)
        print('extractor called')
        return inputDF
