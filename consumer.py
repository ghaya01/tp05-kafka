
import json
import csv
from kafka import KafkaConsumer

TOPIC = "campus-sensors"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-campus-monitor",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Listening to topic: {TOPIC}")

count = 0
total_temperature = 0.0

try:
    with open("sensor_data.csv", "w", newline="", encoding="utf-8") as csvfile:

        fieldnames = [
            "event_id",
            "sensor_id",
            "temperature_c",
            "humidity_pct",
            "timestamp"
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for message in consumer:
            event = message.value

            building = event.get("campus_building", "Unknown")

            # Filter: only Building A
            if building != "Building A":
                continue

            temperature = event["temperature_c"]

            count += 1
            total_temperature += temperature
            average_temperature = total_temperature / count

            # Save event to CSV
            writer.writerow({
                "event_id": event["event_id"],
                "sensor_id": event["sensor_id"],
                "temperature_c": temperature,
                "humidity_pct": event["humidity_pct"],
                "timestamp": event["timestamp"]
            })

            csvfile.flush()

            print(
                f'{event["timestamp"]} | {event["sensor_id"]} | '
                f'{building} | T={temperature}°C | '
                f'RH={event["humidity_pct"]}%'
            )

            print(f"Running average: {average_temperature:.2f}°C")

            if temperature >= 35:
                print("ALERT: High temperature detected!")

except KeyboardInterrupt:
    print("Consumer stopped by user.")

finally:
    consumer.close()
