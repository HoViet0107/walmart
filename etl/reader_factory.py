class Datasource:
    def __init__(self, spark, path):
        self.spark = spark
        self.path = path
       
    def read_df(self):
        raise ValueError('Not implemented')


class CSVDataSource(Datasource):
    def read_df(self):
        print('CSVDataSource class called' )
        return (
            self.spark.read.csv(self.path, header=True, inferSchema=True)
        )


def get_data_source(spark,data_type, path):
    if data_type == 'csv':
        return CSVDataSource(spark, path).read_df()
    else:
        raise ValueError(f'Not implimented this data type: {data_type}')


