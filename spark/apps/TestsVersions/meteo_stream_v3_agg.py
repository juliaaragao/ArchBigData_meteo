from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

scala_version = "2.13"
spark_version = "4.0.1"

packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
]

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MeteoStreamingAgg") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.cores.max", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1) Lecture du topic Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "meteo") \
    .option("startingOffsets", "latest") \
    .load()

# 2) Conversion bytes -> string
df_string = df_kafka.selectExpr("CAST(value AS STRING) AS json_value")

# 3) Schéma météo
meteo_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("geo_id_insee", StringType(), True),
    StructField("reference_time", StringType(), True),
    StructField("insert_time", StringType(), True),
    StructField("validity_time", StringType(), True),
    StructField("t", DoubleType(), True),
    StructField("td", DoubleType(), True),
    StructField("u", IntegerType(), True),
    StructField("dd", IntegerType(), True),
    StructField("ff", DoubleType(), True),
    StructField("rr_per", DoubleType(), True),
    StructField("pres", DoubleType(), True)
])

# 4) Parsing JSON
df_parsed = df_string.select(
    from_json(col("json_value"), meteo_schema).alias("data")
).select("data.*")

# 5) Nettoyage / transformation
df_clean = df_parsed.select(
    col("geo_id_insee"),
    col("lat"),
    col("lon"),
    to_timestamp(col("reference_time")).alias("reference_time"),
    (col("t") - 273.15).alias("temperature_c"),
    col("u").alias("humidity"),
    col("ff").alias("wind_speed"),
    col("dd").alias("wind_direction"),
    col("rr_per").alias("rain"),
    col("pres").alias("pressure")
).filter(col("geo_id_insee").isNotNull())

# 6) Agrégation par station
df_agg = df_clean.groupBy("geo_id_insee").agg(
    avg("temperature_c").alias("avg_temperature_c"),
    avg("humidity").alias("avg_humidity"),
    avg("wind_speed").alias("avg_wind_speed"),
    count("*").alias("nb_messages")
)

# 7) Affichage console
query = df_agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()