from pyspark.sql import SparkSession
from etl.config.aws_config import AWSConfig
import os
import urllib.request

class SparkInitializer:
    _spark = None

    @staticmethod
    def get_spark():
        if SparkInitializer._spark is None:
            # Path to JARs
            jars_dir = os.path.join(os.path.dirname(__file__), "..", "jars")
            jar_files = [
                os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
                os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.262.jar")
            ]
            
            # Ensure all JARs exist
            for jar in jar_files:
                if not os.path.exists(jar):
                    raise FileNotFoundError(f"Missing required JAR: {jar}")

            # Join JAR paths
            jars = ",".join(jar_files)

            # AWS Config
            aws_config = AWSConfig()

            # Initialize Spark session
            SparkInitializer._spark = (SparkSession.builder
                .appName("Walmart Fashion ETL")
                .config("spark.jars", jars)  # Load JARs
                .config("spark.hadoop.fs.s3a.access.key", aws_config.access_key)
                .config("spark.hadoop.fs.s3a.secret.key", aws_config.secret_key)
                .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_config.region}.amazonaws.com")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .getOrCreate())

        return SparkInitializer._spark
    
    @staticmethod
    def stop_spark():
        """
        Stop spark session.
        """
        if SparkInitializer._spark is not None:
            try:
                SparkInitializer._spark.stop()
                SparkInitializer._spark = None
            except Exception as e:
                print(f"Error stopping SparkSession: {str(e)}")