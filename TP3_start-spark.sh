cd spark
docker compose up -d
docker exec spark-history bash /opt/spark/sbin/start-history-server.sh
cd ../consumer
docker compose -f docker-compose.yml up -d