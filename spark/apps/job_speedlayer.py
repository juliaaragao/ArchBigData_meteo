from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, from_utc_timestamp
from pyspark.sql.types import *

scala_version = "2.13"
spark_version = "4.0.1"


# packages 
packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1"  
]

# SparkSession config pour kafka et Cassandra
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


# Kafka source - ici on lit directement du topic "meteo" pour faire la speed layer 
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "meteo")
    .option("startingOffsets", "latest")
    .load()
)

df_string = df_kafka.selectExpr("CAST(value AS STRING) AS json")

# Schéma pour parser le JSON 
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

# Parsing JSON
df_parsed = df_string.select(
    from_json(col("json"), meteo_schema).alias("data")
).select("data.*")

# Nettoyage et transformation des données
df_clean = df_parsed.select(
    col("geo_id_insee"),
    to_timestamp(col("reference_time"), "yyyy-MM-dd'T'HH:mm:ssX").alias("reference_time"),
    #from_utc_timestamp(to_timestamp(col("reference_time")),"Europe/Paris").alias("reference_time"),
    (col("t") - 273.15).alias("temperature_c"),
    col("u").alias("humidity"),
    (col("ff") * 3.6).alias("wind_speed"), # m/s -> km/h
    col("dd").alias("wind_direction"),
    col("rr_per").alias("rain"),
    col("pres").alias("pressure")
).filter(col("geo_id_insee").isNotNull())

# Pour une vue presque temps réel 
# Comme les donnés arrivent toutes 6min regulierment -> 6min fenetre + 7min watermark (testé avec 10 et 12, mais était trop long pour les tests)
# calcul de la moyenne des données météo (temperature, wind et humidity) par station (geo_id_insee) et par fenetre de 6min
df_speed = df_clean \
    .withWatermark("reference_time", "7 minutes") \
    .groupBy(
        col("geo_id_insee"),
        window(col("reference_time"), "6 minutes")
    ).agg(
        avg("temperature_c").alias("avg_temp"),
        avg("wind_speed").alias("avg_wind"),
        avg("humidity").alias("avg_humidity")
    )

# Sélection des colonnes finales et filtrage pour éviter les fenêtres sans données
df_speed_final = df_speed.select(
    col("geo_id_insee"),
    col("window.start").alias("window_start"),
    col("avg_temp"),
    col("avg_wind"),
    col("avg_humidity")
).filter(col("window_start").isNotNull())

# Écriture dans Cassandra
query = (
    df_speed_final.writeStream
    .foreachBatch(lambda df, epoch_id:
        df.write
          .format("org.apache.spark.sql.cassandra")
          .options(table="speed", keyspace="meteo")
          .mode("append")
          .save()
    )
    # .outputMode("append") 
    .outputMode("update")
    .trigger(processingTime="1 minute")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/speed_to_cassandra")
    .start()
)


query.awaitTermination()