import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer


def generate_sensor_data():
    """Generates synthetic IoT sensor data."""
    sensor_ids = ["sensor_001", "sensor_002", "sensor_003", "sensor_004", "sensor_005"]

    sensor_id = random.choice(sensor_ids)
    # Temperature in Celsius (15-35°C range for typical indoor/outdoor monitoring)
    temperature = round(random.uniform(15.0, 35.0), 2)
    # Humidity percentage (30-90% range)
    humidity = round(random.uniform(30.0, 90.0), 2)

    return {
        "sensor_id": sensor_id,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": datetime.now().isoformat(),
    }


def run_producer():
    """Kafka producer that sends synthetic sensor data to the 'sensor-data' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            sensor_data = generate_sensor_data()
            print(f"[Producer] Sending sensor reading #{count}: {sensor_data}")

            future = producer.send("sensor-data", value=sensor_data)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            sleep_time = random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
