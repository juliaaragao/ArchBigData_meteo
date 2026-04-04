# Big Data Project — Lambda Architecture (Météo)

## Structure principale du projet

```bash
.
├── cassandra/              # Configuration Cassandra (docker-compose mono)
├── kafka/                  # Configuration Kafka
├── producer/               # Producer Kafka (envoi des données météo)
├── spark/
│   └── apps/
│       ├── job_ingestion.py    # RAW layer (Kafka → Parquet)
│       ├── job_speedlayer.py   # Speed layer (Streaming → Cassandra)
│       └── job_batchlayer.py   # Batch layer (Parquet → Cassandra)
├── grafana/                # Configuration Grafana
├── data/                   # Données stockées (Parquet)
└── README.md
```

## Description

* **cassandra/** - contient le déploiement de Cassandra (mode mono-node)
* **kafka/** - configuration du broker Kafka
* **producer/** - script qui envoie les données météo dans Kafka
* **spark/apps/** - jobs Spark :
  * ingestion (RAW)
  * streaming (speed layer)
  * batch (historique)
* **grafana/** - dashboard et visualisation


## Architecture
API → Kafka → Spark → (RAW / Speed / Batch) → Cassandra → Grafana

RAW layer → stockage brut (Parquet)
Speed layer → temps réel (Kafka → Spark → Cassandra)
Batch layer → historique (Parquet → Spark → Cassandra)
Serving layer → Cassandra → Grafana

## Containers a activer
1. kafka - Le producer est lancé automatiquement avec Kafka 
```docker-compose up --build -d```

2. spark
```docker-compose up --build -d```


3. cassandra
```docker-compose -f docker-compose_mono.yml up -d```


4. grafana
```docker-compose up --build -d```



## Lancement du RAW, Speed layer et Batch Layer

1. Valider création des donnes sur le topic meteo
```docker logs -f producer```

2. Spark
```docker exec -it spark-master bash```


Pour la partie raw data en parquet
```spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1   /opt/spark-apps/job_ingestion.py```

Pour le speed layer
```spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1   /opt/spark-apps/job_speedlayer.py```

Pour le batch layer
```spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1   /opt/spark-apps/job_batchlayer.py```


Cassandra
```docker exec -it cassandra bash```

```SELECT * FROM meteo.speed;```

```SELECT * FROM meteo.batch_meteo;```

Grafana
acceder a: http://localhost:3000
dashboards > teste


## Notes importantes
- Le producer envoie des données toutes les ~6 minutes
- Le speed layer doit rester actif pour voir les updates
- Si aucune donnée ne s’affiche, supprimer le checkpoint :
```rm -rf /opt/spark-data/checkpoints/speed_to_cassandra```
