import json
from kafka import KafkaConsumer
from pymongo import MongoClient

def consume_sightings():
    print("Starting Kafka consumer...")
    
    # 1. Connect to Kafka
    try:
        consumer = KafkaConsumer(
            'observations',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest', # Start from the beginning if it's a new consumer group
            enable_auto_commit=True,
            group_id='dionis-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000 # Wait for 5 seconds for new messages, then exit (good for Snakemake)
        )
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return

    # 2. Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["dionis"]
    obs_collection = db["observations"]
    species_collection = db["species"]
    
    count = 0
    print("Listening for messages...")
    
    for message in consumer:
        sighting = message.value
        species_name = sighting.get("species_name")
        
        # 3. Try to link with Species Backbone
        # We look for the species in our collection to get its canonical info
        species_ref = species_collection.find_one({"name": species_name})
        if species_ref:
            sighting["species_id"] = species_ref["_id"]
            print(f"Linked sighting to known species: {species_name}")
        else:
            print(f"Sighting of unknown species: {species_name}")
        
        # 4. Save to MongoDB
        obs_collection.insert_one(sighting)
        count += 1
        print(f"Saved sighting at {sighting.get('location')} to database.")

    print(f"Finished. Consumed {count} sightings.")
    client.close()

if __name__ == "__main__":
    consume_sightings()
