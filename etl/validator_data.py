from pyspark.sql.functions import col, count


class Validator:
    def __init__(self):
        pass

    def is_col_contain_null(self, df):
        pass

    def drop_cols(self, df, *cols):
        pass

    def duplicate_records_df(self, df, cols=None):
        pass

    def drop_duplicate_records_df(self, df, cols=None):
        pass

    def convert_data_type_format(self, df, col_map):
        pass


class ValidatorImpl(Validator):
    def is_col_contain_null(self, df):
        """
        :param df: DataFrame
        :return: DataFrame
        """
        df_cols = df.schema.names
        for c in df_cols:
            null_count = df.filter((col(c).isNull()) | (col(c) == "")) \
                .count()
            print(c + ': ', null_count)

    def drop_cols(self, df, cols):
        """
        :param df: DataFrame
        :param cols: danh sách cột cần xóa
        :return: dataset frame
        Example:
        cols = ['zip_code','state']
        drop_cols(df, *cols).printSchema()
        """
        df = df.drop(*cols)
        return df

    def duplicate_records_df(self, df, cols=None):
        """
        :param df: DataFrame
        :param cols: Danh sách cột
        :return: DataFrame
        duplicate_records_df(pur_df, ['purchase_id']).show()
        duplicate_records_df(pur_df, ['customer_id','product_id', 'purchase_date','quantity']).show()
        """
        if cols is None:
            cols = df.columns  # Nếu không có cột nào được chỉ định, dùng tất cả các cột trong DataFrame
        return df.groupBy(cols) \
            .agg(count("*") \
                 .alias("duplicate_count")) \
            .filter(col("duplicate_count") > 1)

    def drop_duplicate_records_df(self, df, *cols):
        """
        :param df: DataFrame
        :param cols: Danh Sách cột
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
        print('validator called')
        print('Type of: ', type(col_map))
        for col_name, data_type in col_map.items():
            print(col_name, data_type)
            df = df.withColumn(col_name, col(col_name).cast(data_type))
        return df
