"""Крок 3. Обробка даних датчиків та генерація сповіщень.

Читає building_sensors і відправляє:
  - у temperature_alerts, якщо температура > 40°C;
  - у humidity_alerts, якщо вологість > 80% або < 20%.
"""

import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from configs import (
    BUILDING_SENSORS_TOPIC,
    HUMIDITY_ALERTS_TOPIC,
    HUMIDITY_MAX,
    HUMIDITY_MIN,
    MY_NAME,
    TEMPERATURE_ALERTS_TOPIC,
    TEMPERATURE_MAX,
    connection_kwargs,
)
from serializers import JsonDeserializer, JsonSerializer, StrDeserializer, StrSerializer


def build_alert(reading: dict, message: str) -> dict:
    return {
        "sensor_id": reading["sensor_id"],
        "timestamp": reading["timestamp"],
        "alert_time": datetime.now(timezone.utc).isoformat(),
        "temperature": reading["temperature"],
        "humidity": reading["humidity"],
        "message": message,
    }


def main() -> None:
    consumer = KafkaConsumer(
        BUILDING_SENSORS_TOPIC,
        **connection_kwargs(),
        value_deserializer=JsonDeserializer(),
        key_deserializer=StrDeserializer(),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"{MY_NAME}_alerts_processor",
    )
    producer = KafkaProducer(
        **connection_kwargs(),
        value_serializer=JsonSerializer(),
        key_serializer=StrSerializer(),
    )
    print(f"Processing '{BUILDING_SENSORS_TOPIC}' (Ctrl+C to stop)")

    try:
        for msg in consumer:
            reading = msg.value
            temperature = reading["temperature"]
            humidity = reading["humidity"]
            print(f"Received: {reading}")

            if temperature > TEMPERATURE_MAX:
                alert = build_alert(
                    reading,
                    f"Temperature {temperature}°C exceeds the threshold of {TEMPERATURE_MAX}°C",
                )
                producer.send(TEMPERATURE_ALERTS_TOPIC, key=str(uuid.uuid4()), value=alert)
                print(f"  -> temperature alert sent")

            if humidity > HUMIDITY_MAX or humidity < HUMIDITY_MIN:
                bound = f"above {HUMIDITY_MAX}%" if humidity > HUMIDITY_MAX else f"below {HUMIDITY_MIN}%"
                alert = build_alert(
                    reading,
                    f"Humidity {humidity}% is {bound} (allowed range {HUMIDITY_MIN}-{HUMIDITY_MAX}%)",
                )
                producer.send(HUMIDITY_ALERTS_TOPIC, key=str(uuid.uuid4()), value=alert)
                print(f"  -> humidity alert sent")

            producer.flush()
    except KeyboardInterrupt:
        print("\nProcessor stopped.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
