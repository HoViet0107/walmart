# Imports
from etl.spark_initializer import SparkInitializer

class Loader:
    def __init__(self):
        pass
    def load_to_db(self, df, table_name, db_config, connection_properties):
        pass
    
class LoadDataToMysql(Loader):
    def load_to_db(self, df, table_name, db_config, connection_properties):
        spark = SparkInitializer.get_spark()

        '''
            .mode("append"): Ghi thêm dữ liệu vào bảng MySQL, không xóa dữ liệu cũ.
            'truncate', 'false': Đảm bảo dữ liệu không bị cắt ngắn.
            'rewriteBatchedStatements', 'true': Cho phép gửi nhiều câu lệnh insert vào MySQL cùng lúc.
            'batchsize', 5000: Chia dữ liệu thành các batch nhỏ để tối ưu hiệu suất(mỗi lần gửi 5 dòng) 
        '''
        
        try:
            df.write.mode(db_config.get('mode')) \
                .option('truncate', 'false') \
                .option('rewriteBatchedStatements', 'true') \
                .option('batchsize', 5000) \
                .jdbc(url=db_config.get('jdbc_url'), 
                    table=table_name, 
                    properties=connection_properties)
            
        except Exception as e:
            print(f"Lỗi khi ghi dữ liệu vào MySQL: {e}")
        