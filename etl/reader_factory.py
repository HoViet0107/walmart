from etl.spark_initializer import SparkInitializer

class Datasource:
    def __init__(self, path):
        self.spark = SparkInitializer.get_spark()
        self.path = path
       
    def read_df(self):
        raise ValueError('Not implemented')


class CSVDataSource(Datasource):
    def read_df(self):
        return (
            self.spark.read.csv(self.path, header=True, inferSchema=True)
        )


def get_data_source(data_type, path):
    if data_type == 'csv':
        return CSVDataSource( path).read_df()
    else:
        raise ValueError(f'Not implimented this data type: {data_type}')


