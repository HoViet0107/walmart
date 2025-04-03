import os
import sys

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# from etl.reader_factory import get_data_source
from etl.spark_initializer import SparkInitializer
from etl.config.aws_config import AWSConfig

class Extractor:
    def __init__(self):
        self.spark = SparkInitializer.get_spark()

    def extract(self):
        raise ValueError('Not implemented')


class DataFrameExtractor(Extractor):
    def extract(self, data_type, path):
        spark = SparkInitializer.get_spark()
        # Get AWS configuration
        aws_config = AWSConfig()
        
        s3_path = f"s3a://{aws_config.bucket_name}/{path}"
        # Read data from S3 using Spark
        return spark.read.format(data_type).options(
            header='true',
            inferSchema='true') \
        .load(s3_path)
