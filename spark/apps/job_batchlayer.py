from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, avg, to_date, coalesce
from pyspark.sql.types import *
from pyspark.sql.functions import get_json_object
from pyspark.sql.functions import coalesce

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MeteoBatchLayer") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# READ PARQUET
# -----------------------------
df_raw = spark.read.parquet("/opt/spark-data/raw/meteo")

print("==== RAW DATA ====")
df_raw.printSchema()
df_raw.show(5, False)

# -----------------------------
# SCHEMA (ATUALIZADO API)
# -----------------------------
meteo_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("geo_id_insee", StringType(), True),
    StructField("reference_time", StringType(), True),

    # 🔥 NOVOS CAMPOS
    StructField("t", DoubleType(), True),
    StructField("t_10", DoubleType(), True),
    StructField("t_20", DoubleType(), True),
    StructField("t_50", DoubleType(), True),

    StructField("u", DoubleType(), True),
    StructField("ff", DoubleType(), True),
    StructField("dd", DoubleType(), True),
    StructField("rr_per", DoubleType(), True),
    StructField("pres", DoubleType(), True)
])

# -----------------------------
# DETECT JSON COLUMN
# -----------------------------
if "json" in df_raw.columns:
    json_col = "json"
elif "value" in df_raw.columns:
    json_col = "value"
else:
    raise Exception("Coluna JSON não encontrada")

# -----------------------------
# PARSE JSON
# -----------------------------
df_parsed = df_raw.select(
    get_json_object(col(json_col), "$.geo_id_insee").alias("geo_id_insee"),
    get_json_object(col(json_col), "$.reference_time").alias("reference_time"),
    get_json_object(col(json_col), "$.t_10").cast("double").alias("t_10"),
    get_json_object(col(json_col), "$.t").cast("double").alias("t"),
    get_json_object(col(json_col), "$.u").cast("double").alias("humidity"),
    get_json_object(col(json_col), "$.ff").cast("double").alias("wind_speed")
)

# -----------------------------
# CLEANING (ROBUSTO)
# -----------------------------

df_clean = df_parsed.select(
    col("geo_id_insee"),
    to_timestamp(col("reference_time"), "yyyy-MM-dd'T'HH:mm:ssX").alias("reference_time"),
    (coalesce(col("t_10"), col("t")) - 273.15).alias("temperature_c"),
    col("humidity"),
    (col("wind_speed") * 3.6).alias("wind_speed")  # conversão de m/s para km/h
).filter(
    col("geo_id_insee").isNotNull() &
    col("reference_time").isNotNull()
)

print("==== CLEAN DATA ====")
print("CLEAN COUNT:", df_clean.count())
df_clean.select("reference_time").show(20, False)

# -----------------------------
# BATCH AGGREGATION
# -----------------------------
df_batch = df_clean.groupBy(
    col("geo_id_insee"),
    to_date(col("reference_time")).alias("date")
).agg(
    avg("temperature_c").alias("avg_temp"),
    avg("wind_speed").alias("avg_wind"),
    avg("humidity").alias("avg_humidity")
)

print("==== BATCH RESULT ====")
print("BATCH COUNT:", df_batch.count())
df_batch.orderBy("date").show(50, False)

# -----------------------------
# WRITE PARQUET
# -----------------------------
df_batch.write \
    .mode("append") \
    .parquet("/opt/spark-data/batch/meteo_aggregated")

print("Written to Parquet")

# -----------------------------
# WRITE CASSANDRA
# -----------------------------
df_batch.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("keyspace", "meteo") \
    .option("table", "batch_meteo") \
    .save()

print("Written to Cassandra")

# -----------------------------
# STOP
# -----------------------------
spark.stop()