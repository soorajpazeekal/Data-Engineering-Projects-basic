import findspark
findspark.init()

import configparser, logging as log
from pyspark.sql import SparkSession # type: ignore

import os



spark = SparkSession.builder \
    .appName("mysql_elt_job") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.22") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3").getOrCreate()

log.basicConfig(level=log.INFO)

'''Snowflake connection options'''
sfOptions = {
  "sfURL" : os.environ['SFURL'],
  "sfUser" : os.environ['SFUSER'],
  "sfPassword" : os.environ['SFPASSWORD'],
  "sfDatabase" : "TEST_DB",
  "sfSchema" : "PUBLIC",
  "sfWarehouse" : "gx_wh"
}

try:
    df = spark.read.csv("./files/*.csv", sep=",", inferSchema="true", header="true")
    df.write.format("net.snowflake.spark.snowflake") \
                    .options(**sfOptions) \
                    .option("dbtable", "sf_final") \
                    .mode("append").save()
    log.info(f"Database operations completed")
    
except Exception as e:
    log.error(e)
df.printSchema()
spark.stop()
