# Imports
from etl.spark_initializer import SparkInitializer
from etl.config.aws_config import AWSConfig

class Loader:
    def __init__(self):
        pass
    def load_to_db(self, df, table_name, db_config, connection_properties):
        pass
    def load_to_s3(self, df, s3_folder_name):
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
            
    def load_to_s3(self, df, s3_folder_name):
        """
        Save transformed data to S3 as a Parquet file.
        """
        try:
            # Initialize Spark
            spark = SparkInitializer.get_spark()
            
            # Get AWS configuration
            aws_config = AWSConfig()
            
            # Define the S3 path for the Parquet file
            s3_path = f"s3a://{aws_config.bucket_name}/parquet_data/{s3_folder_name}/"
            
            # Write the DataFrame to S3 in Parquet format
            df.write.mode("overwrite").parquet(s3_path)
            
            print(f"Data successfully saved to S3 as Parquet at: {s3_path}")
        
        except Exception as e:
            print(f"Error saving data to S3 as Parquet: {e}")
        