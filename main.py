from etl.extractor.extractor import DataFrameExtractor
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from etl.validator_data import ValidatorImpl

# # Tạo Spark Session
spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "./connector/mysql-connector-java-8.0.30.jar") \
    .getOrCreate()


# B1: Trích xuất dữ liệu từ các nguồn
# B1: Trích xuất dữ liệu từ các nguồn
customer_df = (DataFrameExtractor(spark)
                    .extract("csv", "dataset/customer.csv"))

fas_purchase_df = (DataFrameExtractor(spark)
                    .extract("csv", "dataset/fashion_purchase_history.csv"))

product_df = (DataFrameExtractor(spark)
                    .extract("csv", "dataset/products.csv"))
# kiểm tra kiểu dữ liệu
print('\033[1m' +"Kiểu dữ liệu:")
customer_df.printSchema()
fas_purchase_df.printSchema()
product_df.printSchema()

# kiểm tra null
print('\033[1m' + "Null:")
ValidatorImpl().check_null_values(customer_df)
ValidatorImpl().check_null_values(fas_purchase_df)
ValidatorImpl().check_null_values(product_df)

# kiểm tra giá trị lặp
print('\033[1m' +"Bản ghi lặp:")
ValidatorImpl().check_duplicate_records(customer_df,['customer_id', 'first_name', 'last_name', 'gender', 'date_of_birth'])
ValidatorImpl().check_duplicate_records(fas_purchase_df,['customer_id', 'item_purchased', 'date_purchase'])
ValidatorImpl().check_duplicate_records(product_df,['item', 'category'])


# B2: Triển khai logic chuyển đổi(transformation logic)
