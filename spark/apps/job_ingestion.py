from pyspark.sql import SparkSession

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MeteoIngestionBatch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Lecture Kafka (BATCH)
# -----------------------------
df_kafka = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "meteo") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# -----------------------------
# Cast JSON brut
# -----------------------------
df_raw = df_kafka.selectExpr("CAST(value AS STRING) AS json")

# -----------------------------
# Nettoyage minimal (important)
# -----------------------------
df_raw = df_raw.filter("json IS NOT NULL")

# -----------------------------
# Écriture RAW (historique)
# -----------------------------
df_raw.write \
    .mode("append") \
    .parquet("/opt/spark-data/raw/meteo")

spark.stop()