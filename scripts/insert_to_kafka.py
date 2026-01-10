import json
import time
from confluent_kafka import Producer

# Configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def run_inserter():
    topic = "bird-observations"
    
    # Load the specific mock data
    with open('mock/mock_data.json', 'r') as f:
        observations = json.load(f)

    print(f"Starting Kafka Producer. Sending {len(observations)} messages...")

    for obs in observations:
        # Trigger the message delivery
        producer.produce(
            topic, 
            value=json.dumps(obs).encode('utf-8')
        )
        # Briefly poll to handle delivery reports
        producer.poll(0) 

    # Block until all messages are sent
    producer.flush()
    print("Messages successfully delivered to Kafka.")

if __name__ == "__main__":
    run_inserter()