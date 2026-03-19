from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg
from pyspark.sql.types import *

scala_version = "2.13"
spark_version = "4.0.1"

# -----------------------------
# 🔧 MODIFICAÇÃO: adicionando conector Cassandra (versão compatível)
# -----------------------------
packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1"  # versão mais estável
]

# -----------------------------
# 🔧 MODIFICAÇÃO: builder corrigido (sem "\" para evitar erro de sintaxe)
# ➕ conexão com Cassandra
# -----------------------------
# spark = (
#     SparkSession.builder
#     .master("spark://spark-master:7077")
#     .appName("MeteoSpeedLayer")
#     .config("spark.jars", "/opt/spark/custom-jars/spark-cassandra-connector_2.13-3.4.1.jar")  # caminho para o JAR
#     .config("spark.cassandra.connection.host", "cassandra")  # nome do container Docker
#     .getOrCreate()
    
# )
spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName("MeteoSpeedLayer")
    .config(
        "spark.jars.packages",
        "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1"
    )
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.driver.host", "spark-master")  # importante em Docker
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Kafka streaming
# -----------------------------
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "meteo")
    .option("startingOffsets", "latest")
    .load()
)

df_string = df_kafka.selectExpr("CAST(value AS STRING) AS json")

# -----------------------------
# Schema
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

df_parsed = df_string.select(
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
# Speed Layer (agregação tempo real)
# -----------------------------
# df_speed = df_clean.groupBy(
#     col("geo_id_insee"),
#     window(col("reference_time"), "1 minute")
# ).agg(
#     avg("temperature_c").alias("avg_temp"),
#     avg("wind_speed").alias("avg_wind"),
#     avg("humidity").alias("avg_humidity")
# )

df_speed = df_clean \
    .withWatermark("reference_time", "15 minutes") \
    .groupBy(
        col("geo_id_insee"),
        window(col("reference_time"), "10 minutes")
    ).agg(
        avg("temperature_c").alias("avg_temp"),
        avg("wind_speed").alias("avg_wind"),
        avg("humidity").alias("avg_humidity")
    )

# -----------------------------
# ➕ PREPARAÇÃO PARA CASSANDRA
# Cassandra não aceita struct (window), então usamos window.start
# -----------------------------
df_speed_final = df_speed.select(
    col("geo_id_insee"),
    col("window.start").alias("window_start"),
    col("avg_temp"),
    col("avg_wind"),
    col("avg_humidity")
).filter(col("window_start").isNotNull())

# -----------------------------
# 🔧 MODIFICAÇÃO: saída para Cassandra (Serving Layer)
# -----------------------------
query = (
    df_speed_final.writeStream
    .foreachBatch(lambda df, epoch_id:
        df.write
          .format("org.apache.spark.sql.cassandra")
          .options(table="speed", keyspace="meteo")
          .mode("append")
          .save()
    )
    .outputMode("append")  # mais seguro com Cassandra
    .option("checkpointLocation", "/opt/spark-data/checkpoints/speed_to_cassandra")
    .start()
)

query.awaitTermination()