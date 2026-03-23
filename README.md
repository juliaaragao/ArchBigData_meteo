docker logs -f producer

docker exec -it spark-master bash

spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1   /opt/spark-apps/job_speedlayer.py

docker exec -it cassandra bash

SELECT * FROM meteo.speed;

SELECT * FROM batch_meteo;
