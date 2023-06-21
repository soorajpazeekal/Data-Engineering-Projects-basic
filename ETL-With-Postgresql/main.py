import findspark
findspark.init()

import os
import configparser

from pyspark.sql import SparkSession

config = configparser.ConfigParser(); config.read('config.ini')

class PysparkManager:
    def CreateSparkSession(self):
        spark = SparkSession.builder \
            .appName(config.get("DEFAULT", "SparkAppName")) \
            .config("spark.jars", "postgresql-42.6.0.jar") \
            .getOrCreate()
        print("PySpark session created")
        return spark
    def StopSparkSession(self):
        spark.stop()
        print("PySpark session stopped")


spark = PysparkManager().CreateSparkSession()

def FileExtactPhase(directory = config.get("DEFAULT", "DataFolderName")):
    for item in os.listdir(directory):
        if (item.endswith(".gz")):
                df = spark.read \
                    .format("csv") \
                    .option("compression", "gzip") \
                    .option("header", True) \
                    .load(f"{config.get('DEFAULT', 'DataFolderName')}/{item}")    
                return df

def JdbConnection_postgres(ConnectionUrl, table_name, properties):
    try:
        print("Database Connection Success")
        return spark.read.jdbc(ConnectionUrl, table_name, properties=properties)
    
    except Exception as e:
        print(f"Database Connection Failed:{str(e)}")


if __name__ == "__main__":
    df = FileExtactPhase()
    df.printSchema()

    properties = {
        "driver": config.get("DATABASE", "driver"),
        "user": config.get("DATABASE", "ConnectionUser"),
        "password": config.get("DATABASE", "ConnectionPassword"),
    }

    df = JdbConnection_postgres(ConnectionUrl=config.get("DATABASE", "ConnectionUrl"),
                                        table_name="wealth_accounts_data_summary",
                                        properties=properties)
    df.printSchema()
    PysparkManager.StopSparkSession(spark)
    print("hello")