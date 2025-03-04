from pyspark.sql import SparkSession

class SparkInitializer:
    _spark = None
    
    @staticmethod
    def get_spark():
        """
        Trả về 1 singleton SparkSession.
        Tạo mới nếu chưa tồn tại.
        """
        if SparkInitializer._spark is None:
            try:
                SparkInitializer._spark = (
                    SparkSession
                    .builder
                    .appName("Walmart ETL")
                    # Cấu hình thư viện MySQL Connector để đọc/ghi MySQL
                    .config("spark.jars", "./connector/mysql-connector-java-8.0.30.jar") 
                    # Cấu hình thêm
                    .config("spark.sql.warehouse.dir", "spark-warehouse") #Xác định thư mục warehouse để lưu trữ dữ liệu trong Spark SQL.
                    .config("spark.executor.memory", "2g") # Bộ nhớ cấp cho mỗi executor (2GB).
                    .config("spark.driver.memory", "1g") # Bộ nhớ cấp cho driver Spark (1GB).
                    # Tối ưu hiệu suất bằng cách bật Garbage Collector (G1GC) để quản lý bộ nhớ
                    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                    .getOrCreate()
                )
                
                # Cấu hình logging(giảm bớt log chỉ hiển thị lỗi ERROR)
                SparkInitializer._spark.sparkContext.setLogLevel("ERROR")
                
            except Exception as e:
                error_message = f"Error initializing SparkSession: {str(e)}"
                raise RuntimeError(error_message)
                
        return SparkInitializer._spark
    
    @staticmethod
    def stop_spark():
        """
        Dừng spark session.
        """
        if SparkInitializer._spark is not None:
            try:
                SparkInitializer._spark.stop()
                SparkInitializer._spark = None
            except Exception as e:
                print(f"Error stopping SparkSession: {str(e)}")