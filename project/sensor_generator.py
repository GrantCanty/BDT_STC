import json
import random
import time
import datetime
import sqlite3
from kafka import KafkaProducer

print("I'm running")
import socket
try:
    socket.create_connection(('kafka', 9092), timeout=10)
    print("Kafka is reachable!")
except socket.error as e:
    print(f"Kafka is not reachable: {e}")
# Kafka Setup
KAFKA_TOPIC = "sensor-data"
try:
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",  # Use the Kafka container name
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        request_timeout_ms=30000
    )
    print("Kafka producer initialized successfully")
except Exception as e:
    print(f"Failed to initialize Kafka producer: {e}")
    producer = None 

# Database Setup
DB_PATH = "/app/exported_database.db"  
NEW_DB_PATH = "/app/new_database.db"  # New database for storing data

def initialize_database():
    """Ensure the new database table exists before inserting data."""
    conn = sqlite3.connect(NEW_DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT,
            fill_level INTEGER,
            timestamp TEXT
        )
    """)
    
    conn.commit()
    conn.close()


def match_sensor_id(container_id):
    """Match the given container ID with an existing sensor ID in my_database.db."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT sensor_id FROM bins WHERE container_id = ?", (container_id,))
    result = cursor.fetchone()
    
    conn.close()
    
    return result[0] if result else None

def save_to_new_db(sensor_id, fill_level, timestamp):
    """Insert data into the new database."""
    try:
        conn = sqlite3.connect(NEW_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO sensor_data (sensor_id, fill_level, timestamp) VALUES (?, ?, ?)",
                       (sensor_id, fill_level, timestamp))
        conn.commit()
        conn.close()
    except sqlite3.DatabaseError as e:
        print(f"Database Write Error: {e}")

sensor_fill_levels = {}

container_fill_levels = {}

def generate_sensor_data(container_id):
    """Generate a single sensor data entry with a monotonic fill level."""
    if container_id not in container_fill_levels:
        # Start with a random fill level between 10% and 50%
        container_fill_levels[container_id] = random.randint(5, 65)
    else:
        # Increase by 0-10% or stay the same (plateau effect)
        increase = random.choice([0, random.randint(1, 10)])
        container_fill_levels[container_id] = min(100, container_fill_levels[container_id] + increase)

    return {
        "container_id": container_id,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "fill_level": container_fill_levels[container_id]  # Now follows a monotonic function
    }
# Keep track of the last 10 values per container
last_10_values = {}

def stream_data():
    containers = [i for i in range(1, 121)]  # List of container IDs from 1 to 120
    
    while True:
        for container in containers:
            data = generate_sensor_data(container)  # Generate sensor data for this container
            sensor_id = match_sensor_id(container)  # Retrieve the sensor ID based on the container ID

            if sensor_id:
                data["sensor_id"] = sensor_id
                save_to_new_db(sensor_id, data["fill_level"], data["timestamp"])  # Save data to the database

                # Manage last 10 values for internal processing
                if sensor_id not in last_10_values:
                    last_10_values[sensor_id] = []

                last_10_values[sensor_id].append(data)  # Add the current value to the list
                if len(last_10_values[sensor_id]) > 10:
                    last_10_values[sensor_id].pop(0)  # Keep only the last 10 values

                if producer:
                    try:
                        producer.send(KAFKA_TOPIC, last_10_values[sensor_id])
                        print(f"Data sent to Kafka for sensor_id {sensor_id}: {last_10_values[sensor_id]}")
                    except Exception as e:
                        print(f"Error sending data to Kafka: {e}")
                else:
                    print("Kafka producer is not initialized. Skipping data send.")

        time.sleep(10)

if __name__ == "__main__":
    stream_data()