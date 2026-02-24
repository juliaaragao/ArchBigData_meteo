import json
from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(
        "meteo",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    for msg in consumer:
        d = msg.value
        geo = d.get("geo_id_insee")
        t = d.get("t")
        u = d.get("u")
        ff = d.get("ff")
        rr = d.get("rr_per")
        ref = d.get("reference_time")

        # t em Kelvin -> Celsius (se existir)
        t_c = (t - 273.15) if isinstance(t, (int, float)) else None

        print(f"{ref} geo={geo} T={t_c:.2f}°C U={u}% FF={ff} RR={rr}")

if __name__ == "__main__":
    main()