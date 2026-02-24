import json
import time
import urllib.request
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from kafka import KafkaProducer, KafkaClient 
from kafka.admin import KafkaAdminClient, NewTopic
import os

def main():
    """_summary_
    
    Returns:
        _type_: _description_
    """    
    API_KEY = os.getenv("API_KEY") # FIXME Set your own API key here
    #url = " ".format(API_KEY)
    #url = "https://public-api.meteofrance.fr/public/DPObs/station/infrahoraire-6m"

    id_station = "29075001"
    url = f"https://public-api.meteofrance.fr/public/DPObs/v1/station/infrahoraire-6m?id_station={id_station}&format=json"

    # topic = sys.argv[1]
    topic = 'meteo'
    
    kafka_client = KafkaClient(bootstrap_servers='kafka:29092')
    
    admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    server_topics = admin.list_topics()

    #topic = "meteo"
    num_partition = 1

    print(server_topics)
    # création du topic si celui-ci n'est pas déjà créé
    if topic not in server_topics:
        try:
            print("create new topic :", topic)

            topic1 = NewTopic(name=topic,
                             num_partitions=num_partition,
                             replication_factor=1)
            admin.create_topics([topic1])
        except Exception:
            print("error")
            pass
    else:
        print(topic,"est déjà créé")

    producer = KafkaProducer(bootstrap_servers="kafka:29092")

    # while True:
    #     response = urllib.request.urlopen(url)
    #     stations = json.loads(response.read().decode())
    #     print(len(stations))
    #     for station in stations:
    #         producer.send(topic, json.dumps(station).encode())
            
    #     print("{} Produced {} station records".format(datetime.fromtimestamp(time.time()), len(stations)))
    #     time.sleep(60)

    while True:
        req = urllib.request.Request(
            url,
            headers={
                 "accept": "*/*",
                 "apikey": API_KEY,
},
        )

        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode("utf-8"))

    
        observations = data

        print("Qtd observações:", len(observations))

        for obs in observations:
            producer.send(topic, json.dumps(obs).encode("utf-8"))

        # print("{} Produced {} records".format(
        #     # datetime.fromtimestamp(time.time()),
        #     datetime.now(),
        #     len(observations))
        # )

        print("{} Produced {} records".format(
            datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M:%S"),
            len(observations)
            ))

        time.sleep(360)

if __name__ == "__main__":
    main()
