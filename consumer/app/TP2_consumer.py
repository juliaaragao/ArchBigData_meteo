import json
from kafka import KafkaConsumer, TopicPartition

def main():
    print("Démarrage du consumer...")

    consumer = KafkaConsumer(
        bootstrap_servers="kafka:29092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    tp = TopicPartition("meteo", 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)

    print("Partition assignée manuellement:", tp)
    print("Lecture des messages...")

    got_any = False

    for msg in consumer:
        got_any = True
        print("Message brut reçu:", msg.value)

        d = msg.value
        geo = d.get("geo_id_insee")
        t = d.get("t")
        u = d.get("u")
        ff = d.get("ff")
        rr = d.get("rr_per")
        ref = d.get("reference_time")

        t_c = (t - 273.15) if isinstance(t, (int, float)) else None
        temp_str = f"{t_c:.2f}°C" if t_c is not None else "N/A"

        print(f"{ref} geo={geo} T={temp_str} U={u} FF={ff} RR={rr}")

    if not got_any:
        print("Aucun message lu.")

if __name__ == "__main__":
    main()