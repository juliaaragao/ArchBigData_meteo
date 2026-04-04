from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import *


scala_version = "2.13"
spark_version = "4.0.1"

packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
]

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MeteoStreaming") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.cores.max", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "meteo") \
    .option("startingOffsets", "latest") \
    .load()

df_string = df.selectExpr("CAST(value AS STRING)")

query = df_string.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()