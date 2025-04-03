from setuptools import setup, find_packages

setup(
    name="walmart-etl",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pyspark==3.5.0',
        'boto3==1.34.14',
        'python-dotenv==1.0.0',
        'delta-spark==3.0.0'
    ]
)