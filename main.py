from etl.extractor.extractor import DataFrameExtractor
from etl.spark_initializer import SparkInitializer
from pyspark.sql.types import *
from etl.validator_data import ValidatorImpl
from etl.tranform.transform import CustomerDataTransformer,PurchaseHistoryDataTransformer,ProductDataTransformer
from etl.load.loader import LoadDataToMysql
from etl.sql_executor import MysqlExecutor

spark = SparkInitializer.get_spark()



### B1: Extract data from csv file
fas_purchase_df = (DataFrameExtractor()
                    .extract('csv', 'dataset/fashion_purchase_history.csv'))
customer_df = (DataFrameExtractor()
                    .extract('csv', 'dataset/customer.csv'))
product_df = (DataFrameExtractor()
                    .extract('csv', 'dataset/products.csv'))
# Data type
print('\033[1m' +"Data types:")
customer_df.printSchema()
fas_purchase_df.printSchema()
product_df.printSchema()

# Check Check missing value
print('\033[1m' + "Null:")
ValidatorImpl().check_null_values(customer_df)
ValidatorImpl().check_null_values(fas_purchase_df)
ValidatorImpl().check_null_values(product_df)

# Check duplicate reacords
print('\033[1m' +"Duplicate records:")
ValidatorImpl().check_duplicate_records(customer_df,['customer_id', 'first_name', 'last_name', 'gender', 'date_of_birth'])
ValidatorImpl().check_duplicate_records(fas_purchase_df,['customer_id', 'item_purchased', 'date_purchase'])
ValidatorImpl().check_duplicate_records(product_df,['item', 'category'])
#-------------------end B1------------------------



# Step 2: Transform
'''
- Transform data type:
    - date_of_birth: TimeStamp -> Date
    - date_purchase: Date -> Timestamp
    - review_rating: Double -> Floatx
- Drop duplicates
- Handle missing value
'''
cleand_customer_df = CustomerDataTransformer().transform(customer_df)
cleand_fas_purchase_df = PurchaseHistoryDataTransformer().transform(fas_purchase_df)
cleand_product_df = ProductDataTransformer().transform(product_df)

# Check data type again
cleand_customer_df.printSchema()
cleand_fas_purchase_df.printSchema()
cleand_product_df.printSchema()

# Check duplicate value again
ValidatorImpl().check_duplicate_records(cleand_customer_df,['customer_id', 'first_name', 'last_name', 'gender', 'date_of_birth'])
ValidatorImpl().check_duplicate_records(cleand_fas_purchase_df,['customer_id', 'item_purchased', 'date_purchase'])
ValidatorImpl().check_duplicate_records(cleand_product_df,['item', 'category'])
#-------------------end B2------------------------



# Step 3: Load data to MySQL database
## Create table 
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
message = '✅ Creation table success!' 
error_message = '❌ Error:'
for statement in sql_statements:
    try:
        MysqlExecutor().execute([statement], message, error_message)
    except Exception as e:
        print(f"Error executing statement: {statement}")
        print(f"Error details: {e}")
        break

# Load to Mysql database
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
    Checking referential integrity: 
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


# Create purchase_history table
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
MysqlExecutor().execute(sql_statement, message, error_message)
#-------------------end B3------------------------