from pyspark.sql.functions import col, count, isnan, when


class Validator:
    def __init__(self):
        pass

    def check_null_values(self, df):
        pass

    def drop_cols(self, df, *cols):
        pass

    def check_duplicate_records(self, df, cols=None):
        pass

    def drop_duplicate_records_df(self, df, cols=None):
        pass

    def convert_data_type_format(self, df, col_map):
        pass


class ValidatorImpl(Validator):            
    def check_null_values(self,dataframe):
        """
        :param df: DataFrame
        :return: DataFrame
        """
        cols_info = [(c[0], c[1]) for c in dataframe.dtypes]
        
        exprs = []
        for column_name, data_type in cols_info:
            # numeric types, Check null and NaN
            if data_type in ('double', 'float'):
                expr = count(
                        when(
                            isnan(col(column_name)) | col(column_name).isNull(), 
                            column_name
                        )
                    )
            # non-numeric types, only check null
            else:
                expr = count(
                        when(
                            col(column_name).isNull(), 
                            column_name
                        )
                    )
            
            exprs.append(expr.alias(column_name))
    
        dataframe.select(exprs).show()

    def drop_cols(self, df, cols):
        """
        :param df: DataFrame
        :param cols: list columns
        :return: dataset frame
        Example:
        cols = ['zip_code','state']
        drop_cols(df, *cols).printSchema()
        """
        df = df.drop(*cols)
        return df

    def check_duplicate_records(self, df, cols=None):
        """
        :param df: DataFrame
        :param cols: list columns
        :return: DataFrame
        duplicate_records_df(pur_df, ['purchase_id']).show()
        duplicate_records_df(pur_df, ['customer_id','product_id', 'purchase_date','quantity']).show()
        """
        if cols is None:
            cols = df.columns  # if no cols specified, check all
        return df.groupBy(cols) \
            .agg(count("*") \
                 .alias("duplicate_count")) \
            .filter(col("duplicate_count") > 1).show()

    def drop_duplicate_records_df(self, df, *cols):
        """
        :param df: DataFrame
        :param cols: List columns
        :return: DataFrame
        """
        if cols is None:
            raise ValueError('Column cannot be empty!')
        else:
            return df.dropDuplicates(*cols)

    def convert_data_type_format(self,df, col_map):
        """
            data_type_map: Dictionary: {'column_name': data_type}
            df: DataFrame
        """
        for col_name, data_type in col_map.items():
            df = df.withColumn(col_name, col(col_name).cast(data_type))
        return df
