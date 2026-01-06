import json
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def run_inserter():
    topic = "bird-observations"
    # This data uses the birds you already scraped!
    with open('mock/mock_data.json', 'r') as f:
        observations = json.load(f)

    for obs in observations:
        producer.produce(topic, json.dumps(obs).encode('utf-8'))
    
    producer.flush()
    print(f"Successfully sent {len(observations)} sightings to Kafka.")

if __name__ == "__main__":
    run_inserter()