from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains
from etl.validator_data import ValidatorImpl


class Transformer:
    def __init__(self):
        pass

    def transform(self, input_dfs, conditions):
        pass


class DataTransform(Transformer):
    def transform(self, input_dfs, conditions):
        # if (is_contains_null == True):
        if ('drop_cols' in conditions
                and conditions['drop_cols']):
            input_dfs = (ValidatorImpl()
                         .drop_cols(
                            input_dfs,
                            conditions['drop_cols'])
            )

        """
        condition_dict = {
            'drop_dup_record':{
                'contain_dup':True,
                'cols':['col_1', 'col_2']
            }
        }
        """
        if ('drop_dup_record' in conditions
                and conditions['drop_dup_record']['contain_dup']):
            input_dfs = ValidatorImpl().drop_duplicate_records_df(input_dfs, conditions['drop_dup_record'])

        """
        condition_dict = {
            'change_col_type':{
                'is_convert':True,
                'cols':['col_1', 'col_2']
            }
        }
        """
        if ('change_col_type' in conditions
                and conditions['change_col_type']['is_convert']):
            input_dfs = (ValidatorImpl()
                            .convert_data_type_format(
                                input_dfs,
                                conditions['change_col_type']['cols']
                            ))

        return input_dfs
