from flask import Flask, request, jsonify
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import sys
import json

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
topic = 'finantial_data'

# Create producer instance
producer = Producer({'bootstrap.servers': bootstrap_servers})


def create_topic(bootstrap_servers, topic_name, num_partitions=1, replication_factor=1):
    # Create AdminClient using provided bootstrap servers
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Check if the topic already exists
    topic_metadata = admin_client.list_topics(timeout=10).topics
    if topic_name in topic_metadata:
        print(f"Topic '{topic_name}' already exists.")
        return

    # Create a new topic
    new_topic = NewTopic(topic_name, num_partitions, replication_factor)
    result = admin_client.create_topics([new_topic])

    # Wait for topic creation to finish
    for topic, future in result.items():
        try:
            future.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")
# Define a function to delivery report
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Produce JSON data to Kafka topic
def produce_to_kafka(json_data):
    print(json_data)
    # Kafka broker configuration
    bootstrap_servers = 'localhost:9092'
    # Topic name
    topic_name = 'finantial_data'
    # Number of partitions (default: 1)
    num_partitions = 1
    # Replication factor (default: 1)
    replication_factor = 1

    create_topic(bootstrap_servers, topic_name, num_partitions, replication_factor)
    # Produce message to Kafka topic
    producer.produce(topic, value=json_data.encode('utf-8'), callback=delivery_report)
    # Wait for message to be sent
    producer.flush()


app = Flask(__name__)

@app.route('/ingest', methods=['POST'])
def ingest():
    try:
        data = request.json  # Assumes the incoming data is JSON
        #print(f"{data}")
        # Send JSON data to Kafka
        produce_to_kafka(data)
        
        # Print message for demonstration
        print("Sent JSON data to Kafka:", data)
        
        #return jsonify(data)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)