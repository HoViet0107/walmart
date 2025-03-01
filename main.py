from etl.extractor import DataFrameExtractor
from pyspark.sql import SparkSession
from etl.transform import DataTransform
from pyspark.sql.types import *

# # Tạo Spark Session
spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "./connector/mysql-connector-java-8.0.30.jar") \
    .getOrCreate()


# B1: Trích xuất dữ liệu từ các nguồn
customer_df = (DataFrameExtractor(spark)
               .extract("csv", "dataset/customer_profile_dataset.csv"))
purchase_df = (DataFrameExtractor(spark)
               .extract("csv", "dataset/purchase_history_dataset.csv"))
product_df = (DataFrameExtractor(spark)
              .extract("csv", "dataset/products_dataset.csv"))
customer_df.printSchema()
purchase_df.printSchema()
product_df.printSchema()
# customer_df.show()
# customer_df.printSchema()
# B2: Triển khai logic chuyển đổi(transformation logic)
"""
customer_transform_condition = {
            'drop_cols': ['state', 'city'],
            'change_col_type':{
                            'is_convert':True,
                            'cols':{
                                'customer_id': StringType(),
                                'zip_code': StringType()
                                }
                            }
            }
"""
customer_transform_condition = {}
transformed_customer_df = DataTransform().transform(customer_df, customer_transform_condition)
transformed_customer_df.printSchema()

purchase_transform_condition = {
    'drop_cols':['total_amount']
}
transformed_purchase_df = DataTransform().transform(purchase_df, purchase_transform_condition)
transformed_purchase_df.printSchema()

product_transform_condition = {}
transformed_product_df = DataTransform().transform(product_df, product_transform_condition)
transformed_product_df.printSchema()