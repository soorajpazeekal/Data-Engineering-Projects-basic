import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.appName("StructuredStreamingSales").getOrCreate()

host = "localhost"
port = 8888

lines = spark.readStream \
    .format("socket") \
    .option("host", host) \
    .option("port", port) \
    .load()

query = lines.writeStream \
    .outputMode("append") \
    .foreach(lambda row: print(row.value)).start()

query.awaitTermination()
spark.stop()

