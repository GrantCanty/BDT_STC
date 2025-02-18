from confluent_kafka import Consumer, Producer, KafkaError
import json
import time

import socket
try:
    socket.create_connection(('kafka', 9092), timeout=10)
    print("Kafka is reachable!")
except socket.error as e:
    print(f"Kafka is not reachable: {e}")

# Kafka configuration
KAFKA_BROKER = "kafka:9092"  # Kafka broker address
INPUT_TOPIC = "predicted_values"  # Input topic
COLLECT_BINS_TOPIC = "collect_bins"  # Output topic
GROUP_ID = "classifier_group"  # Consumer group ID

# Initialize Kafka consumer
consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)
consumer.subscribe([INPUT_TOPIC])

# Initialize Kafka producer
producer_conf = {"bootstrap.servers": KAFKA_BROKER}
producer = Producer(producer_conf)

# Store sensor IDs with predicted values over 80
sensor_ids_over_80 = set()

def process_message(msg):
    """
    Process a Kafka message and add sensor_id to the set if predicted_value > 80.
    """
    try:
        data = json.loads(msg.value().decode("utf-8"))
        sensor_id = data["sensor_id"]
        predicted_value = data["predicted_value"]

        if predicted_value > 80:
            sensor_ids_over_80.add(sensor_id)
            print(f"Added sensor_id {sensor_id} (predicted_value: {predicted_value})")
    except Exception as e:
        print(f"Error processing message: {e}")

def send_sensor_ids():
    """
    Send the collected sensor_ids to the 'collect_bins' topic every 60 seconds.
    """
    if sensor_ids_over_80:
        message = {"sensor_ids": list(sensor_ids_over_80), "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")}
        producer.produce(COLLECT_BINS_TOPIC, json.dumps(message).encode("utf-8"))
        producer.flush()
        print(f"Sent sensor IDs to {COLLECT_BINS_TOPIC}: {sensor_ids_over_80}")
        sensor_ids_over_80.clear()  # Clear the set after sending
    else:
        print("No sensor IDs to send.")

def main():
    try:
        while True:
            # Poll for new messages
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Process the message
            process_message(msg)

            if int(time.time()) % 15 == 0:
                send_sensor_ids()
                time.sleep(1)   

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()