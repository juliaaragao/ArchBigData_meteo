# python kafka docker
The main objective in this project was to learn how to create an application that sends and receives a message from Kafka, using Docker and docker-compose tools.

## How to use

### Using Docker Compose 
You will need Docker installed to follow the next steps. To create and run the image use the following command:

```bash
cd kafka
docker-compose up -d
cd ../producer
docker-compose up -d
cd ../consumer
docker-compose up -d
```
or

```bash
bash TP2_start-kafka-producer-consumer.sh 
```


The configuration will create 3 clusters with 5 containers:

- Consumer:
   - Consumer container
     - from python:3.11-alpine (version amd64 and arm64/v8)
- Producer
	- Publisher container
	  - from python:3.11-alpine (version amd64 and arm64/v8) 
- Kafka
   - kafka container
      - from confluentinc/cp-kafka:7.9.0 (version amd64 and arm64/v8)
   - kafdrop container
      - from obsidiandynamics/kafdrop:4.1.0 (version amd64 and arm64/v8)
   - zookeeper container
      - from confluentin/cp-zookeeper:7.9.0 (version amd64 and arm64/v8)

container|Exposed ports
---|---
consumer|
producer|8000
kafdrop|19000:9000
kafka|9091 9092
zookeeper|2181

The Publisher container sends data to Kafka.

The Consumer container is a script that aims to wait and receive messages from Kafka.

And the kafdrop container will provide acess to  web UI for viewing Kafka topics and browsing consumer groups that can be accessed at `http://localhost:19000`.

Il faut ouvrir un terminal sur les containers producer et consumer puis lancer le programmer le programme python.

```bash
docker exec -ti producer sh
```

```bash
cd app
python TP2_producer.py velib-station
```
On peut lancer un bash contenant la commande suivante depuis un terminal "localhost"

```bash
bash TP2_exec-producer.sh
```


```bash
docker exec -ti consumer sh
```

```bash
cd app
python TP2_consumer.py
```
On peut lancer un bash contrenant la commande suivante depuis un terminal "localhost"

```bash 
bash TP2_exec-consumer.sh
```

## Project Structure
Below is a project structure created:

```
cmd .
├── README.md
├── kafka
│   ├── docker-compose.yml
├── consumer
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── app
│   │   ├── __init__.py
│   │   └── TP2_consumer.py
│   └── requirements.txt
└── producer
    ├── Dockerfile
    ├── docker-compose.yml
    ├── app
    │   ├── __init__.py
    │   ├── TP2_producer.py
    └── requirements.txt
```



## Help and Resources
You can read more about the tools documentation:

- [Docker](https://docs.docker.com/get-started/overview/)
- [Kafdrop](https://github.com/obsidiandynamics/kafdrop)
- [Kafka](https://kafka.apache.org)
- [Kafka-python](https://kafka-python.readthedocs.io/en/master/)