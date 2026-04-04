import json
from datetime import datetime, timezone
from kafka import KafkaProducer


def main():

    bootstrap = "kafka:29092"

    # topic
    topic = "meteo"

    # creation du producer Kafka
    producer = KafkaProducer(bootstrap_servers=bootstrap)

    # tester fake message
    message = {
        "station_id": "TEST_STATION_001",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_c": 12.3,
        "humidity_pct": 78,
        "wind_kph": 14.2
    }

    producer.send(topic, json.dumps(message).encode("utf-8"))
    producer.flush()
    print(" Sent one message to topic:", topic)
    print(message)


if __name__ == "__main__":
    main()
