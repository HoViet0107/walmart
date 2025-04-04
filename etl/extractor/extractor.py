import os
import sys
from etl.reader_factory import get_data_source

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# from etl.reader_factory import get_data_source
from etl.spark_initializer import SparkInitializer
from etl.config.aws_config import AWSConfig

class Extractor:
    def __init__(self):
        self.spark = SparkInitializer.get_spark()

    def extract_from_s3(self):
        raise ValueError('Not implemented')
    
    def extract_from_csv(self):
        pass


class DataFrameExtractor(Extractor):
    def extract_from_s3(self, data_type, path):
        spark = SparkInitializer.get_spark()
        # Get AWS configuration
        aws_config = AWSConfig()
        
        s3_path = f"s3a://{aws_config.bucket_name}/{path}"
        # Read data from S3 using Spark
        return spark.read.format(data_type).options(
            header='true',
            inferSchema='true') \
        .load(s3_path)
        
    def extract_from_csv(self, data_type, path):
        inputDF = get_data_source(data_type, path)
        return inputDF
