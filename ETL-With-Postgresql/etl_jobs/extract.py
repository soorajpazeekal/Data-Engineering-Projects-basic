import __init__
import configparser

from main import PysparkManager, database_conn_properties, FileExtactPhase, spark

database_url, properties, config = database_conn_properties()

df = FileExtactPhase(config.get("DEFAULT", "DataFolderName"))
df.printSchema()
PysparkManager.StopSparkSession(spark)