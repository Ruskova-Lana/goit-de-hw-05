"""Крок 2. Імітація одного датчика.

Кожен запуск скрипту — це один датчик з випадковим, але сталим у межах
запуску ID. Щоб імітувати декілька датчиків, запустіть скрипт декілька разів.
"""

import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from configs import BUILDING_SENSORS_TOPIC, connection_kwargs
from serializers import JsonSerializer, StrSerializer

SEND_INTERVAL_SEC = 2

SENSOR_ID = random.randint(1000, 9999)


def generate_reading() -> dict:
    return {
        "sensor_id": SENSOR_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(25, 45), 2),
        "humidity": round(random.uniform(15, 85), 2),
    }


def main() -> None:
    producer = KafkaProducer(
        **connection_kwargs(),
        value_serializer=JsonSerializer(),
        key_serializer=StrSerializer(),
    )
    print(f"Sensor {SENSOR_ID} started. Sending to '{BUILDING_SENSORS_TOPIC}' (Ctrl+C to stop)")

    try:
        while True:
            data = generate_reading()
            producer.send(BUILDING_SENSORS_TOPIC, key=str(uuid.uuid4()), value=data)
            producer.flush()
            print(f"Sent: {data}")
            time.sleep(SEND_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\nSensor stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
