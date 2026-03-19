from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, avg, window
from pyspark.sql.types import *

# -----------------------------
# Spark Session + Cassandra config
# -----------------------------
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MeteoBatchLayer") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# RAW data (Parquet)
# -----------------------------
df_raw = spark.read.parquet("/opt/spark-data/raw/meteo")

# -----------------------------
# Schema JSON
# -----------------------------
meteo_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("geo_id_insee", StringType(), True),
    StructField("reference_time", StringType(), True),
    StructField("t", DoubleType(), True),
    StructField("u", DoubleType(), True),
    StructField("ff", DoubleType(), True),
    StructField("dd", DoubleType(), True),
    StructField("rr_per", DoubleType(), True),
    StructField("pres", DoubleType(), True)
])

# -----------------------------
# Parse JSON
# -----------------------------
df_parsed = df_raw.select(
    from_json(col("json"), meteo_schema).alias("data")
).select("data.*")

# -----------------------------
# Cleaning
# -----------------------------
df_clean = df_parsed.select(
    col("geo_id_insee"),
    to_timestamp(col("reference_time"), "yyyy-MM-dd'T'HH:mm:ssX").alias("reference_time"),
    (col("t") - 273.15).alias("temperature_c"),
    col("u").alias("humidity"),
    col("ff").alias("wind_speed"),
    col("dd").alias("wind_direction"),
    col("rr_per").alias("rain"),
    col("pres").alias("pressure")
).filter(col("geo_id_insee").isNotNull())

# -----------------------------
# DEBUG (muito importante 👇)
# -----------------------------
print("RAW COUNT:", df_raw.count())
print("CLEAN COUNT:", df_clean.count())
df_clean.show(5)

# -----------------------------
# BATCH = agregação histórica
# -----------------------------
df_batch = df_clean.groupBy(
    col("geo_id_insee"),
    window(col("reference_time"), "1 day")
).agg(
    avg("temperature_c").alias("avg_temp"),
    avg("wind_speed").alias("avg_wind"),
    avg("humidity").alias("avg_humidity")
)

# -----------------------------
# Flatten (Cassandra não aceita struct)
# -----------------------------
df_batch_final = df_batch.select(
    col("geo_id_insee"),
    col("window.start").alias("date"),
    col("avg_humidity"),
    col("avg_temp"),
    col("avg_wind")
)

# -----------------------------
# DEBUG (ESSENCIAL)
# -----------------------------
print("BATCH COUNT:", df_batch_final.count())
df_batch_final.show(5)

# -----------------------------
# Write 1: Parquet (Batch Layer)
# -----------------------------
df_batch_final.write \
    .mode("append") \
    .parquet("/opt/spark-data/batch/meteo_aggregated")

print("Written to Parquet")

# -----------------------------
# Write 2: Cassandra (Serving Layer)
# -----------------------------
df_batch_final.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("keyspace", "meteo") \
    .option("table", "batch_meteo") \
    .save()

print("Written to Cassandra")

# -----------------------------
# Stop
# -----------------------------
spark.stop()