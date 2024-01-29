import configparser

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
        spark = SparkSession.builder.master("local[*]") \
            .appName(config.get("DEFAULT", "SparkAppName")) \
            .config("spark.jars", "postgresql-42.6.0.jar") \
            .getOrCreate()
        print("PySpark session created")
        return spark
    def StopSparkSession(self, spark):
        spark.stop()
        print("PySpark session stopped")


"""
Extracts data from a list of csv files present in a specified directory (default: DataFolderName).
:param directory: A string representing the path of the directory containing the csv files.
:return: A dataframe containing the data from the csv file/files.
"""

def FileExtractPhase(spark, directory = config.get("DEFAULT", "DataFolderName")):
    df = spark.read \
        .format("csv") \
        .option("compression", "gzip") \
        .option("header", True) \
        .option("inferSchema", "true") \
        .option("path", "{}/*.gz".format(config.get("DEFAULT", "DataFolderName"))) \
        .load()
    return df



def write_database(data_frame, table_name, database_url, properties, move_file=False):
    try:
        #df.write.jdbc(url=database_url, properties=properties, table=table, mode="overwrite")
        data_frame.write.format("jdbc") \
            .option("url", database_url) \
            .option("dbtable", "warehouse.{}".format(table_name)) \
            .mode("overwrite") \
            .options(**properties) \
            .save()
        print("Database Write Success")
        return data_frame
    except Exception as e:
        print("An error occurred:", str(e))




def read_database(spark, table_name, database_url, properties):
    try:
        df = spark.read.format("jdbc") \
            .option("url", database_url) \
            .option("dbtable", "warehouse.{}".format(table_name)) \
            .options(**properties) \
            .load()
        print("Database Read Success")
        return df
    except Exception as e:
        print("An error occurred:", str(e))



if __name__ == "__main__":
    spark = PysparkManager().CreateSparkSession()
    database_url, properties, config = database_conn_properties()
    df = FileExtractPhase(spark)
    df.cache()
    df = read_database(spark=spark, table_name="test", database_url=database_url, properties=properties)
    df.printSchema()
    print(df.count())
    #write_database(data_frame=df, table_name="test", database_url=database_url, properties=properties)
    PysparkManager.StopSparkSession(self=spark, spark=spark)