from pyspark.sql.functions import col
from pyspark.sql.types import *
from etl.validator_data import ValidatorImpl


class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDF):
        pass


class CustomerDataTransformer(Transformer):
    def transform(self, inputDF):
        # chuyển đổi kiểu dữ liệu
        '''
            {
                'date_of_birth': DateType(),
                'date_of_birth': DateType()
            }
        '''
        conver_dtype_df = ValidatorImpl().convert_data_type_format(
            inputDF,
            {'date_of_birth': DateType()}
            )
        
        # fill null
        '''
        chỉ điền giá trị mặc định thì c1
        c1: 
            customer_df.fillna('Unknown', subset=['first_name', 'last_name', 'address', 'city', email]) \
                        .fillna('1990-01-01', subset=['date_of_birth']) \
                        .fillna('000-000-0000', subset=['phone_number']) 


        c2: mỗi lần điền thì sẽ tạo 1 df với mỗi cột, nếu có hơn 1 đk xác định thì dùng c2
            fillna = customer_df
                .withColumn('first_name', when(col('first_name').isNull(), 'Unknown').otherwise('first_name'))
                .withColumn('last_name', when(col('last_name').isNull(), 'Unknown').otherwise('last_name'))
                .withColumn('address', when(col('address').isNull(), 'Unknown').otherwise('address'))
                .withColumn('email', when(col('email').isNull(), 'Unknown').otherwise('email'))
                .withColumn('date_of_birth', when(col('date_of_birth').isNull(), '1990-01-01').otherwise('date_of_birth'))
                .withColumn('phone_number', when(col('phone_number').isNull(), '000-000-0000').otherwise('phone_number'))
        '''
        fill_null_df = conver_dtype_df.fillna('Unknown', subset=['first_name', 'last_name', 'address', 'city', 'email']) \
                                    .fillna('1990-01-01', subset=['date_of_birth']) \
                                    .fillna('000-000-0000', subset=['phone_number']) \
                                    .fillna('Other', subset=['gender'])
        # loại bỏ lặp
        outputDF = ValidatorImpl().drop_duplicate_records_df(fill_null_df, ['customer_id', 'first_name', 'last_name', 'gender', 'date_of_birth'])
        return outputDF

class PurchaseHistoryDataTransformer(Transformer):
    def transform(self, inputDF):
        # chuyển đổi kiểu dữ liệu
        conver_dtype_df = ValidatorImpl().convert_data_type_format(
            inputDF,
                {
                    'date_purchase': TimestampType(),
                    'review_rating': FloatType()
                }
            )
        
        # fill null
        fill_null_df = conver_dtype_df.fillna(-1, subset=['purchase_amount', 'review_rating'])
        
        # loại bỏ lặp
        outputDF = ValidatorImpl().drop_duplicate_records_df(fill_null_df, ['customer_id', 'item_purchased', 'date_purchase'])
        return outputDF

class ProductDataTransformer(Transformer):
    def transform(self, inputDF):
        # chuyển đổi kiểu dữ liệu
        # fill null
        outputDF = inputDF.fillna('Unknow_category', subset=['category'])
        # loại bỏ lặp

        return outputDF
    
    
'''
    chỉ điền giá trị mặc định thì c1
    c1: 
        customer_df.fillna('Unknown', subset=['first_name', 'last_name', 'address', 'city', email]) \
                    .fillna('1990-01-01', subset=['date_of_birth']) \
                    .fillna('000-000-0000', subset=['phone_number']) 


    c2: mỗi lần điền thì sẽ tạo 1 df với mỗi cột, nếu có hơn 1 đk xác định thì dùng c2
        fillna = customer_df
        .withColumn('first_name', when(col('first_name').isNull(), 'Unknown').otherwise('first_name'))
        .withColumn('last_name', when(col('last_name').isNull(), 'Unknown').otherwise('last_name'))
        .withColumn('address', when(col('address').isNull(), 'Unknown').otherwise('address'))
        .withColumn('email', when(col('email').isNull(), 'Unknown').otherwise('email'))
        .withColumn('date_of_birth', when(col('date_of_birth').isNull(), '1990-01-01').otherwise('date_of_birth'))
        .withColumn('phone_number', when(col('phone_number').isNull(), '000-000-0000').otherwise('phone_number'))
'''