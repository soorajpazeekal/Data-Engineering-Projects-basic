import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Apply transformations as needed
# For example, you can split the lines into words

# Define a sink to write the DataFrame (e.g., console sink)
query = lines.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination to keep the streaming job running
query.awaitTermination()