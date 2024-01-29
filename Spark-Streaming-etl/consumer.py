import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, BooleanType
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

spark = SparkSession.builder.appName("StructuredStreamingSales").getOrCreate()


host = "localhost"
port = 8888
cluster = Cluster([host])
session = cluster.connect('live_feed')

json_schema = StructType([
    StructField("Order_id", StringType(), False),  
    StructField("Name", StringType(), False),
    StructField("Email", StringType(), False),  
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("created_at", FloatType(), True),
    StructField("Total_Price", FloatType(), True),
    StructField("Discount_or_Promotion", BooleanType(), True),
    StructField("PaymentMethod", StringType(), True)
])

data_df = spark.createDataFrame([], json_schema)


lines = spark.readStream \
    .format("socket") \
    .option("host", host) \
    .option("port", port) \
    .load()


json_data = lines.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), json_schema).alias("json_data")).select("json_data.*")
#json_data = json_data.withColumn("some_column", col("some_column").cast(DesiredDataType))


table = "live_table"
insert_query = session.prepare(f"""
    INSERT INTO {table} (order_id, Name, Email, latitude, longitude, created_at, Total_Price, Discount_or_Promotion, PaymentMethod) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

batch = BatchStatement()


def write_to_db(df):
    try:
        for row in df.rdd.collect():
            batch.add(insert_query, (row.Order_id, row.Name, row.Email, row.latitude, row.longitude, row.created_at, row.Total_Price, row.Discount_or_Promotion, row.PaymentMethod))
        session.execute(batch)
    except Exception as e:
        print(f"Error: {e}")


def write_to_dataframe(batch_df, batch_id):
    global data_df
    data_df = data_df.union(batch_df)
    write_to_db(data_df)
    data_df.show()

query = json_data.writeStream \
    .queryName("Final to DB") \
    .outputMode("append") \
    .foreachBatch(write_to_dataframe) \
    .trigger(processingTime="30 seconds").start()


query.awaitTermination()
session.shutdown()
cluster.shutdown()
spark.stop()

