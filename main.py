from etl.extractor.extractor import DataFrameExtractor
from etl.spark_initializer import SparkInitializer
from pyspark.sql.types import *
from etl.validator_data import ValidatorImpl
from etl.tranform.transform import CustomerDataTransformer,PurchaseHistoryDataTransformer,ProductDataTransformer
from etl.load.loader import LoadDataToMysql
from etl.sql_executor import MysqlExecutor

spark = SparkInitializer.get_spark()



### B1: Trích xuất dữ liệu từ các nguồn

fas_purchase_df = (DataFrameExtractor()
                    .extract('csv', 'dataset/fashion_purchase_history.csv'))
customer_df = (DataFrameExtractor()
                    .extract('csv', 'dataset/customer.csv'))
product_df = (DataFrameExtractor()
                    .extract('csv', 'dataset/products.csv'))
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
#-------------------end B1------------------------



# Bước 2: Chuyển đổi dữ liệu
'''
- Chuyển đổi kiểu dữ liệu:
    - date_of_birth: TimeStamp -> Date
    - date_purchase: Date -> Timestamp
    - review_rating: Double -> Floatx
- Loại bỏ lặp
- Sửa bản ghi có giá trị null
'''
cleand_customer_df = CustomerDataTransformer().transform(customer_df)
cleand_fas_purchase_df = PurchaseHistoryDataTransformer().transform(fas_purchase_df)
cleand_product_df = ProductDataTransformer().transform(product_df)

# kiểm tra kiểu dữ liệu
cleand_customer_df.printSchema()
cleand_fas_purchase_df.printSchema()
cleand_product_df.printSchema()

# kiểm tra giá trị lặp
ValidatorImpl().check_duplicate_records(cleand_customer_df,['customer_id', 'first_name', 'last_name', 'gender', 'date_of_birth'])
ValidatorImpl().check_duplicate_records(cleand_fas_purchase_df,['customer_id', 'item_purchased', 'date_purchase'])
ValidatorImpl().check_duplicate_records(cleand_product_df,['item', 'category'])
#-------------------end B2------------------------



# Bước 3: Tải dữ liệu vào MySQL
## Tạo bảng 
sql_statements =[
    """
    CREATE TABLE customer(
        customer_id INT PRIMARY KEY AUTO_INCREMENT,
        first_name VARCHAR(20),
        last_name VARCHAR(20),
        gender VARCHAR(10),
        date_of_birth DATE,
        email VARCHAR(50),
        phone_number VARCHAR(12),
        signup_date TIMESTAMP, 
        address VARCHAR(255),
        city VARCHAR(50)    
    )
    """,
    """
    CREATE TABLE product (
        item VARCHAR(50) PRIMARY KEY,
        category VARCHAR(50)
    )
    """,
    """
    CREATE TABLE purchase_history_stagging (
        purchase_id INT PRIMARY KEY AUTO_INCREMENT,
        customer_id INT,
        item_purchased VARCHAR(50),
        purchase_amount DOUBLE,
        quantity INT,
        date_purchase TIMESTAMP,
        review_rating FLOAT,
        payment_method VARCHAR(15)
       )   
    """    
]
# Tạo bảng
message = '✅ Tạo bảng thành công!' 
error_message = '❌ Lỗi khi tạo bảng:'
for statement in sql_statements:
    try:
        MysqlExecutor().execute([statement], message, error_message)
    except Exception as e:
        print(f"Error executing statement: {statement}")
        print(f"Error details: {e}")
        break

# Tải vào Mysql
db_config = {
    'mode': 'append',
    'jdbc_url': 'jdbc:mysql://localhost:3306/walmart'
}
connection_properties = {
    'user': 'root',
    'password': '12345',
    'driver': 'com.mysql.cj.jdbc.Driver'
}

customer_table_name = 'customer'
LoadDataToMysql().load_to_db(cleand_customer_df, customer_table_name, db_config, connection_properties)

product_table_name = 'product'
LoadDataToMysql().load_to_db(cleand_product_df, product_table_name, db_config, connection_properties)

purchase_table_name = 'purchase_history_stagging'
LoadDataToMysql().load_to_db(cleand_fas_purchase_df, purchase_table_name, db_config, connection_properties)

'''
    Kiểm tra dữ liệu tham chiếu giữa bảng: 
        purchase_history và product
        purchase_history và customer
'''
# missing product
missing_product = cleand_fas_purchase_df.join(
    cleand_product_df,
    cleand_fas_purchase_df.item_purchased == cleand_product_df.item,
    'left_anti'
)
missing_product.show()
# missing customer
missing_customer = cleand_fas_purchase_df.join(
    cleand_customer_df,
    cleand_fas_purchase_df.customer_id == cleand_customer_df.customer_id ,
    'left_anti'
)
missing_customer.show()


# Tạo bảng có ràng buộc 
sql_statement=["""
    CREATE TABLE purchase_history 
    SELECT ph.* 
    FROM purchase_history_stagging AS ph
    JOIN Product AS p 
    ON ph.item_purchased = p.item;
    """,
    
    """
    ALTER TABLE purchase_history
    ADD CONSTRAINT fk_customer_id 
    FOREIGN KEY (customer_id) 
    REFERENCES customer(customer_id) 
    ON DELETE CASCADE;
    """,
    
    """
    ALTER TABLE purchase_history
    ADD CONSTRAINT fk_item_purchased 
    FOREIGN KEY (item_purchased) 
    REFERENCES product(item) 
    ON DELETE CASCADE;   
    """
]
message = '✅ Tạo bảng purchase_history thành công' 
error_message = '❌ Lỗi khi tạo bảng:'
MysqlExecutor().execute(sql_statement, message, error_message)
#-------------------end B3------------------------