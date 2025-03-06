# Imports
from etl.spark_initializer import SparkInitializer

class Loader:
    def __init__(self):
        pass
    def load_to_db(self, df, table_name, db_config, connection_properties):
        pass
    
class LoadDataToMysql(Loader):
    def load_to_db(self, df, table_name, db_config, connection_properties):
        spark = SparkInitializer.get_spark()

        '''
            .mode("append"): add new data to table but not overwrite.
            'rewriteBatchedStatements', 'true': combines multiple insert statements into batches.
            'batchsize', 5000: nums of rows each batch
        '''
        
        try:
            df.write.mode(db_config.get('mode')) \
                .option('truncate', 'false') \
                .option('rewriteBatchedStatements', 'true') \
                .option('batchsize', 5000) \
                .jdbc(url=db_config.get('jdbc_url'), 
                    table=table_name, 
                    properties=connection_properties)
            
        except Exception as e:
            print(f"Error loading data to MySQL: {e}")
        