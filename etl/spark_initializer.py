from pyspark.sql import SparkSession

class SparkInitializer:
    _spark = None
    
    @staticmethod
    def get_spark():
        """
        Return a singleton SparkSession.
        Create new if not exist.
        """
        if SparkInitializer._spark is None:
            try:
                SparkInitializer._spark = (
                    SparkSession
                    .builder
                    .appName("Walmart ETL")
                    # configure MySQL Connector to read/write data to MySQL
                    .config("spark.jars", "./connector/mysql-connector-java-8.0.30.jar") 
                    .config("spark.sql.warehouse.dir", "spark-warehouse")
                    .config("spark.executor.memory", "2g") # Specify executor memory (2GB).
                    .config("spark.driver.memory", "1g") # Specify driver Spark memory (1GB).
                    # Enable Garbage Collector (G1GC)
                    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                    .getOrCreate()
                )
                
                # Configure logging
                SparkInitializer._spark.sparkContext.setLogLevel("ERROR")
                
            except Exception as e:
                error_message = f"Error initializing SparkSession: {str(e)}"
                raise RuntimeError(error_message)
                
        return SparkInitializer._spark
    
    @staticmethod
    def stop_spark():
        """
        Stop spark session.
        """
        if SparkInitializer._spark is not None:
            try:
                SparkInitializer._spark.stop()
                SparkInitializer._spark = None
            except Exception as e:
                print(f"Error stopping SparkSession: {str(e)}")