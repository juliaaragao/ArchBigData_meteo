from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MeteoBatchV1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Lecture du parquet...")

df = spark.read.parquet("/opt/spark-data/meteo_parquet")

print("Schema :")
df.printSchema()

print("Exemple de données :")
df.show(20, False)

print("Agrégation par station :")

result = df.groupBy("geo_id_insee").agg(
    count("*").alias("nb_observations"),
    avg("temperature_c").alias("avg_temperature_c"),
    avg("humidity").alias("avg_humidity")
)

result.show()

spark.stop()