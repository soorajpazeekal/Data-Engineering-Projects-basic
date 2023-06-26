import findspark
findspark.init()

import os
import configparser
from pyspark.sql import SparkSession

#Database auth properties
config = configparser.ConfigParser(); config.read('config.ini')
database_url = config.get("DATABASE", "ConnectionUrl")
properties = {
    "driver": config.get("DATABASE", "driver"),
    "user": config.get("DATABASE", "ConnectionUser"),
    "password": config.get("DATABASE", "ConnectionPassword")
}

#Main declaration for pyspark manager
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

"""
Extracts data from a list of csv files present in a specified directory (default: DataFolderName).
:param directory: A string representing the path of the directory containing the csv files.
:return: A dataframe containing the data from the csv file/files.
"""
def FileExtactPhase(directory = config.get("DEFAULT", "DataFolderName")):
    df = spark.read \
        .format("csv") \
        .option("compression", "gzip") \
        .option("header", True) \
        .option("path", "{}/*.gz".format(config.get("DEFAULT", "DataFolderName"))) \
        .load()
    return df



if __name__ == "__main__":
    df = FileExtactPhase()
    df.printSchema()
    print(df.count())

    database_df = spark.read.jdbc(url=database_url, table="test_table", properties=properties); print(database_df.count())
    # df.write.jdbc(url=config.get("DATABASE", "ConnectionUrl"), properties=properties, table="test_table",mode="overwrite")
    # print("Database Write Success")


    PysparkManager.StopSparkSession(spark)