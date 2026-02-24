# Spark docker

# General

A simple spark standalone cluster for your testing environment purposses. A *docker-compose up* away from you solution for your spark development environment.

The Docker compose will create a cluster spark composed by the following containers:

container|Exposed ports
---|---
spark-master|4040 7001:700 7077 9080:8080
spark-worker-a|7002:7000 9082:8080
spark-worker-b|7003:7000 9083:8080
spark-worker-c|7004:7000 9084:8080
spark-history|18080

# Installation

The following steps will make you run your spark cluster's containers.


## Build the image


```sh
bash TP3_spark.sh
```

```bash
cd soark
docker-compose up -d
```


## Validate your cluster

Just validate your cluster accesing the spark UI on each worker & master URL.

### Spark Master

http://localhost:9080/

![alt text](./images/spark-master.png "Spark master UI")

### Spark Worker a

http://localhost:9082/

![alt text](./images/spark-worker-a.png "Spark worker 1 UI")

### Spark History

http://localhost:18080/

![alt text](./images/spark-history.png "Spark worker 2 UI")


# Resource Allocation 

This cluster is shipped with three workers and one spark master, each of these has a particular set of resource allocation(basically RAM & cpu cores allocation).

* The default CPU cores allocation for each spark worker is 2 core.

* The default RAM for each spark-worker is 1024 MB.

* The default RAM allocation for spark executors is 256mb.

* The default RAM allocation for spark driver is 128mb

* If you wish to modify this allocations just edit the env/spark-worker.sh file.

# Binded Volumes

To make app running easier I've shipped two volume mounts described in the following chart:

Host Mount|Container Mount|Purposse
---|---|---
apps|/opt/spark-apps|Used to make available your app's jars on all workers & master
data|/opt/spark-data| Used to make available your app's data on all workers & master

This is basically a dummy DFS created from docker Volumes...(maybe not...)

# Run Sample applications

```sh
docker exec -it tp3_docker-spark-worker-a bash
```

To submit the app connect to one of the workers or the master and execute:

```sh
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--driver-memory 1G \
--executor-memory 1G \
/opt/spark-apps/TP3_exercice1_DataFrame.py
```

```sh
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
/opt/spark-apps/TP3_exercice1_DataFrame.py
```

```sh
python3 /opt/spark-apps/TP3_exercice1_DataFrame.py
```

