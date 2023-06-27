import os
import configparser
import shutil

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


#Database auth properties
config = configparser.ConfigParser(); config.read('config.ini')
def database_conn_properties():
    database_url = config.get("DATABASE", "ConnectionUrl")
    properties = {
        "driver": config.get("DATABASE", "driver"),
        "user": config.get("DATABASE", "ConnectionUser"),
        "password": config.get("DATABASE", "ConnectionPassword")
    }
    return database_url, properties, config

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

def write_database(df, table, move_file=False):
    try:
        df.write.jdbc(url=database_url, properties=properties, table=table, mode="overwrite")
        print("Database Write Success")
        return df
    except Exception as e:
        print("An error occurred:", str(e))


def read_database(table):
    try:
        df = spark.read.jdbc(url=database_url, properties=properties, table=table)
        print("Database Read Success")
        return df
    except Exception as e:
        print("An error occurred:", str(e))
