from pyspark.sql import SparkSession

# Configurations Spark
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MeteoIngestionBatch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# kafka source -> ici on lit tout le topic "meteo" pour faire une ingestion batch (historique)
df_kafka = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "meteo") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Conversion du message Kafka
df_raw = df_kafka.selectExpr("CAST(value AS STRING) AS json")

# Filtre pour éviter les messages vides
df_raw = df_raw.filter("json IS NOT NULL")

# Stockage des données brutes (RAW layer)
# Ces données servent de source de vérité pour les traitements batch ultérieurs
df_raw.write \
    .mode("append") \
    .parquet("/opt/spark-data/raw/meteo")

spark.stop()