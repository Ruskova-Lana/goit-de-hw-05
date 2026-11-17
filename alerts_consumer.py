"""Крок 4. Читання сповіщень з temperature_alerts та humidity_alerts і вивід на екран."""

from kafka import KafkaConsumer

from configs import (
    HUMIDITY_ALERTS_TOPIC,
    MY_NAME,
    TEMPERATURE_ALERTS_TOPIC,
    connection_kwargs,
)
from serializers import JsonDeserializer, StrDeserializer


def main() -> None:
    consumer = KafkaConsumer(
        TEMPERATURE_ALERTS_TOPIC,
        HUMIDITY_ALERTS_TOPIC,
        **connection_kwargs(),
        value_deserializer=JsonDeserializer(),
        key_deserializer=StrDeserializer(),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"{MY_NAME}_alerts_consumer",
    )
    print(f"Listening to '{TEMPERATURE_ALERTS_TOPIC}' and '{HUMIDITY_ALERTS_TOPIC}' (Ctrl+C to stop)")

    try:
        for msg in consumer:
            alert = msg.value
            kind = "TEMPERATURE" if msg.topic == TEMPERATURE_ALERTS_TOPIC else "HUMIDITY"
            print(
                f"[{kind} ALERT] sensor={alert['sensor_id']} "
                f"time={alert['timestamp']} "
                f"temperature={alert['temperature']}°C "
                f"humidity={alert['humidity']}% | {alert['message']}"
            )
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
