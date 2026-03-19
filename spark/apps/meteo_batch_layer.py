from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_trunc,
    avg,
    min,
    max,
    sum,
    count
)

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MeteoBatchLayer") \
    .config("spark.cores.max", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1) Lecture de l'historique complet produit par la speed layer
df = spark.read.parquet("/opt/spark-data/meteo_parquet")

# 2) Sécurité minimale : garder les lignes valides
df_clean = df.filter(
    col("geo_id_insee").isNotNull() &
    col("reference_time").isNotNull()
)

# 3) Création d'une granularité horaire
df_hourly = df_clean.withColumn(
    "hour_bucket",
    date_trunc("hour", col("reference_time"))
)

# 4) Agrégations batch sur tout l'historique
df_batch = df_hourly.groupBy("geo_id_insee", "hour_bucket").agg(
    avg("temperature_c").alias("avg_temperature_c"),
    min("temperature_c").alias("min_temperature_c"),
    max("temperature_c").alias("max_temperature_c"),
    avg("humidity").alias("avg_humidity"),
    avg("wind_speed").alias("avg_wind_speed"),
    sum("rain").alias("total_rain"),
    avg("pressure").alias("avg_pressure"),
    count("*").alias("nb_observations")
)

# 5) Écriture du résultat batch
df_batch.write \
    .mode("overwrite") \
    .parquet("/opt/spark-data/meteo_batch_agg")

print("Batch layer terminée : résultats écrits dans /opt/spark-data/meteo_batch_agg")

spark.stop()
