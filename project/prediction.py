import json
from kafka import KafkaConsumer, KafkaProducer
import sqlite3
from statsmodels.tsa.holtwinters import SimpleExpSmoothing
import numpy as np
import pandas as pd

print("I'm running")


# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "sensor-data"
OUTPUT_TOPIC = "predicted_values"
import socket
try:
    socket.create_connection(('kafka', 9092), timeout=10)
    print("Kafka is reachable!")
except socket.error as e:
    print(f"Kafka is not reachable: {e}")

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="ml-consumer-group"
)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Store the last 10 data points for each sensor
last_10_values = {}

def get_geo_id(sensor_id):
    database_path = "exported_database.db"
    geo_id = None
    
    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT geo_id FROM bins WHERE sensor_id = ?", (sensor_id,))
        result = cursor.fetchone()
        
        # Store the result in geo_id if found
        if result:
            geo_id = result[0]
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()
    
    return geo_id

def predict_smoothing(time_series, slope):
    time_series = np.asarray(time_series).flatten()
    model = SimpleExpSmoothing(time_series)
    fitted_model= model.fit(smoothing_level=0.6, optimized=False)
    
    # Predict next 3 values
    predictions = fitted_model.forecast(3)
    # Modify predictions based on geo_id
    adjusted_predictions = [round(pred + (100 * slope)) for pred in predictions]
    
    return adjusted_predictions


def predict_time_series(data, geo_id):
    """
    Perform prediction using the ML model.
    """
    # Reshape the data to match the model's input shape (1D array of 10 elements for each sensor)
    input_data = np.array(data).reshape(1, -1)  # Convert list to a 2D array (1, 10)
    slope = get_slope(geo_id)
    prediction = predict_smoothing(input_data, slope)
    return prediction[2]  # Return the last prediction

def get_slope(geo_id):
    slope = None
    try:
        df = pd.read_csv("slopes_with_geo_id.csv")
        slope_value = df.loc[df['geo_id'] == geo_id, 'slope']
        if not slope_value.empty:
            slope = slope_value.values[0]
    except Exception as e:
        print(f"Error reading slope file: {e}")
    return slope

def process_sensor_data():
    """
    Consume data from Kafka, process it, and send predictions to Kafka.
    """
    for message in consumer:
        try:
            # Extract sensor data from the Kafka message
            for data_point in message.value:
                sensor_id = data_point["sensor_id"]
                timestamp = data_point["timestamp"]
                fill_level = data_point["fill_level"]

                # Initialize the list for the sensor if it doesn't exist
                if sensor_id not in last_10_values:
                    last_10_values[sensor_id] = []

                # Add the new data point to the list
                last_10_values[sensor_id].append({
                    "timestamp": timestamp,
                    "fill_level": fill_level
                })

                # Keep only the last 10 data points for each sensor
                if len(last_10_values[sensor_id]) > 10:
                    last_10_values[sensor_id].pop(0)

                # Perform prediction when we have 10 data points
                if len(last_10_values[sensor_id]) == 10:
                    # Extract the fill levels for prediction
                    fill_levels = [data["fill_level"] for data in last_10_values[sensor_id]]

                    # Perform prediction using the ML model
                    geo_id = get_geo_id(sensor_id)
                    predicted_value = predict_time_series(fill_levels, geo_id)

                    # Prepare the prediction result
                    prediction_result = {
                        "sensor_id": sensor_id,
                        "predicted_value": predicted_value,
                        "timestamp": timestamp  # Use the latest timestamp
                    }

                    # Send the prediction to the output Kafka topic
                    producer.send(OUTPUT_TOPIC, prediction_result)
                    producer.flush()  # Ensure the message is sent immediately

                    print(f"Prediction for sensor_id {sensor_id}: {prediction_result}")

        except Exception as e:
            print(f"Error processing sensor data: {e}")
if __name__ == "__main__":
    print("Starting ML prediction consumer...")
    process_sensor_data()