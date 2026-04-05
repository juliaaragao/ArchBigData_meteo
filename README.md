# Big Data Project — Lambda Architecture (Météo)

Ce projet met en oeuvre un pipeline de traitement de données météorologiques reposant sur une Lambda Architecture. 

Les observations sont collectées de l'API publique de Météo France et transitent par un système d'ingestion continu avant d'être traitées selon deux approches complémentaires : 
- traitement en temps réel pour la réactivité
- traitement batch pour la consolidation des résultats sur l'historique. 

Nous avons utilisé un environnement conteneurisé et s'appuie sur des outils Kafka, Spark, Cassandra et grafana. 


## Architecture globale

```
API Météo France → Kafka → Spark → (RAW -> Batch / Speed) → Cassandra → Grafana
```


## Structure principale du projet 

```bash
.
├── cassandra/              # Configuration Cassandra (docker-compose mono)
├── kafka/                  # Configuration Kafka + producer
├── producer/               # Producer Kafka (envoi des données météo)
├── spark/
│   └── apps/
│       ├── job_ingestion.py    # RAW layer  
│       ├── job_speedlayer.py   # Speed layer 
│       └── job_batchlayer.py   # Batch layer 
├── grafana/                # Configuration Grafana
├── data/                   # Données stockées (Parquet)
└── README.md
```

> Certains fichiers présents dans le dépôt ont été repris et adaptés à partir de TPs réalisés en cours et, donc, ils peuvent ne pas être utilisés directement dans le pipeline final. 
> Les fichiers listés ci-dessus correspondent aux composants effectivement utilisés.

## Lancement des containers


### 1. Kafka + Producer

```bash
cd kafka
docker-compose up -d --build
```

### 2. Spark

```bash
cd spark
docker-compose up -d --build
```

### 3. Cassandra

```bash
cd cassandra
docker-compose -f docker-compose_mono.yml up -d
```

### 4. Grafana

```bash
cd grafana
docker-compose up -d --build
```


## Lancement des traitements Spark

Dans le container Spark:

```bash
docker exec -it spark-master bash
```

### RAW layer

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1 \
  /opt/spark-apps/job_ingestion.py
```

### Speed layer 

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1 \
  /opt/spark-apps/job_speedlayer.py
```

- Laisser tourner le **speed layer** au moins **15 minutes** (afin de couvrir au moins deux fenêtres de 6 min et le watermark de 7 min), puis **l'arrêter pour libérer les ressources du cluster Spark**, et le relancer une fois le job terminé.

### Batch layer 

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1 \
  /opt/spark-apps/job_batchlayer.py
```


## Vérification

### Kafka — logs du producer

```bash
docker logs -f producer
```

### Cassandra — requêtes CQL

```bash
docker exec -it cassandra cqlsh
```

```sql
SELECT * FROM meteo.speed LIMIT 10;
SELECT * FROM meteo.batch_meteo LIMIT 10;
```

### Grafana

Accéder [http://localhost:3000](http://localhost:3000) → Dashboards → "teste"



## Notes importantes
- Le speed layer doit rester actif en continu pour que les données soient mises à jour dans Cassandra
- En cas de stream bloqué, supprimer le checkpoint :

```bash
rm -rf /opt/spark-data/checkpoints/speed_to_cassandra
```

