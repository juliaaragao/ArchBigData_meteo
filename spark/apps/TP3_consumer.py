from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("TP3_Meteo_Consumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Read from Kafka (stream)
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "meteo")
        .option("startingOffsets", "latest")
        .load()
    )

    # 2) Convert bytes -> string
    raw = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

    # 3) Parse JSON (schema based on your real message)
    schema_ddl = """
        lat DOUBLE,
        lon DOUBLE,
        geo_id_insee STRING,
        reference_time STRING,
        insert_time STRING,
        validity_time STRING,
        t DOUBLE,
        td DOUBLE,
        u DOUBLE,
        dd DOUBLE,
        ff DOUBLE,
        dxi10 DOUBLE,
        fxi10 DOUBLE,
        rr_per DOUBLE,
        t_10 DOUBLE,
        t_20 DOUBLE,
        t_50 DOUBLE,
        t_100 DOUBLE,
        vv DOUBLE,
        etat_sol DOUBLE,
        sss DOUBLE,
        insolh DOUBLE,
        ray_glo01 DOUBLE,
        pres DOUBLE,
        pmer DOUBLE
    """

    parsed = raw.select(F.from_json(F.col("json_str"), schema_ddl).alias("d")).select("d.*")

    # 4) timestamps + convert temperature to Celsius
    df = (
        parsed
        .withColumn("reference_ts", F.to_timestamp("reference_time"))
        .withColumn("t_c", F.col("t") - F.lit(273.15))
        .filter(F.col("reference_ts").isNotNull())
        .filter(F.col("geo_id_insee").isNotNull())
    )

    # 5) Aggregation example: 10-min window per station
    agg = (
        df.withWatermark("reference_ts", "30 minutes")
          .groupBy(F.window("reference_ts", "10 minutes"), F.col("geo_id_insee"))
          .agg(
              F.avg("t_c").alias("avg_temp_c"),
              F.avg("u").alias("avg_humidity"),
              F.avg("ff").alias("avg_wind"),
              F.sum("rr_per").alias("sum_rain")
          )
          .orderBy("window", "geo_id_insee")
    )

    # 6) Output to console (easy validation)
    query = (
        agg.writeStream
           .outputMode("append")
           .format("console")
           .option("truncate", "false")
           .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()