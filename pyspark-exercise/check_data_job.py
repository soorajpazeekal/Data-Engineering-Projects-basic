import findspark
findspark.init()

import configparser, logging as log
from pyspark.sql import SparkSession # type: ignore

import os
import sys
import great_expectations as gx
import pandas as pd


context = gx.get_context()
log.basicConfig(level=log.INFO)


spark = SparkSession.builder \
    .appName("mysql_elt_job") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.22") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3").getOrCreate()

df = spark.read.csv("./files/*.csv", sep=",", inferSchema="true", header="true")

# df.show()

datasource = context.sources.add_spark("my_spark_datasource")

name = "my_df_asset"
data_asset = datasource.add_dataframe_asset(name=name)
my_batch_request = data_asset.build_batch_request(dataframe=df)
print(my_batch_request)


def data_cleaning_steps(validator):
    result = validator.expect_column_values_to_be_unique(column="Year")
    if result["success"] == False:
        log.info("unique data checks failed")
        sys.exit(1)
    result = validator.expect_column_values_to_not_be_null(column="Year")
    if result["success"] == False:
        log.info("null data checks failed")
        return False
    return True

validator = context.get_validator(
    batch_request=my_batch_request
)

data_cleaning_steps(validator)

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
    df.write.format("net.snowflake.spark.snowflake") \
                .options(**sfOptions) \
                .option("dbtable", "sf_csv_action_clean_table") \
                .mode("append").save()
    log.info(f"Database operations completed")
except Exception as e:
    log.error(e)
spark.stop()