import json
import psycopg2
from kafka import KafkaConsumer


def run_consumer():
    """Consumes sensor location messages from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "sensor-locations",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="sensor-locations-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5434",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_locations (
                sensor_id VARCHAR(50) PRIMARY KEY,
                latitude NUMERIC(10, 7),
                longitude NUMERIC(10, 7),
                elevation NUMERIC(10, 2)
            );
            """
        )
        print("[Consumer] ✓ Table 'sensor_locations' ready.")

        print("[Consumer] 🎧 Listening for messages...\n")
        message_count = 0

        for message in consumer:
            try:
                location_data = message.value

                insert_query = """
                    INSERT INTO sensor_locations (sensor_id, latitude, longitude, elevation)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (sensor_id) DO UPDATE SET
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        elevation = EXCLUDED.elevation;
                """
                cur.execute(
                    insert_query,
                    (
                        location_data["sensor_id"],
                        location_data["latitude"],
                        location_data["longitude"],
                        location_data["elevation"],
                    ),
                )
                message_count += 1
                print(
                    f"[Consumer] ✓ #{message_count} Updated sensor {location_data['sensor_id']} | "
                    f"Lat: {location_data['latitude']}, Lon: {location_data['longitude']}, "
                    f"Elev: {location_data['elevation']}m"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
