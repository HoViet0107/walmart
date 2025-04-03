import boto3
from dotenv import load_dotenv
import os

load_dotenv()

class AWSConfig:
    def __init__(self):        
        self.access_key = os.getenv('AWS_ACCESS_KEY')
        self.secret_key = os.getenv('AWS_SECRET_KEY')
        self.region = os.getenv('AWS_REGION', 'ap-southeast-2')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')

    def get_s3_client(self):
        return boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region
        )