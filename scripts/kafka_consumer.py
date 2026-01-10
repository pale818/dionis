from confluent_kafka import Consumer
from pymongo import MongoClient
import json
import os

# Connection to your MongoDB
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["bird_db"]
obs_collection = db["observations"]

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dionis_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['bird-observations'])

def run_consumer():
    print("Consumer started. Waiting for sightings...")
    try:
        # We poll for a few seconds to grab all messages currently in Kafka
        msg_count = 0
        while True:
            msg = consumer.poll(2.0) # Wait 2 seconds for data
            if msg is None: 
                print(f"No more messages. Total saved: {msg_count}")
                break
            
            data = json.loads(msg.value().decode('utf-8'))
            obs_collection.insert_one(data) # Saves the varying biological data [cite: 117]
            msg_count += 1
            print(f"Saved observation for: {data['taxonomy_code']}")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()