import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = SparkSession.builder.appName("StructuredStreamingSales").getOrCreate()

cassandra_options = {
    "keyspace": "store",  # Replace with your Cassandra keyspace name
    "table": "test",  # Replace with your Cassandra table name
    "cluster": "127.0.0.1"  # Replace with your Cassandra cluster IP address
}

host = "localhost"
port = 8888

json_schema = StructType([
    StructField("Order_id", StringType(), False),  
    StructField("Name", StringType(), False),
    StructField("Email", StringType(), False),  
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("created_at", StringType(), True)
])

data_df = spark.createDataFrame([], json_schema)


lines = spark.readStream \
    .format("socket") \
    .option("host", host) \
    .option("port", port) \
    .load()

json_data = lines.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), json_schema).alias("json_data")).select("json_data.*")
#json_data = json_data.withColumn("some_column", col("some_column").cast(DesiredDataType))

def write_to_dataframe(batch_df, batch_id):
    global data_df
    data_df = data_df.union(batch_df)
    data_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(**cassandra_options) \
        .save()

query = json_data.writeStream.outputMode("append").foreachBatch(write_to_dataframe).format("console").start()
#lines.isStreaming()
json_data.printSchema()
all_items = data_df.collect()
query.awaitTermination()
spark.stop()

