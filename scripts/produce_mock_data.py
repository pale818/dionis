import json
import time
from kafka import KafkaProducer

def produce_mock_sightings():
    print("Starting mock sighting producer...")
    
    # Connect to Kafka
    # Note: 'localhost:9092' is used because we are running this script outside Docker 
    # connecting to the container's exposed port.
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return

    # Mock data: "Sparrow at Central Park", "Eagle at Grand Canyon", etc.
    sightings = [
        {"species_name": "Passer domesticus", "location": "Central Park, NY", "lat": 40.7850, "lon": -73.9682, "observer": "John Doe", "habitat": "Urban Park"},
        {"species_name": "Haliaeetus leucocephalus", "location": "Grand Canyon, AZ", "lat": 36.0544, "lon": -112.1401, "observer": "Jane Smith", "flight_pattern": "Soaring"},
        {"species_name": "Turdus migratorius", "location": "Highlands, NJ", "lat": 40.4022, "lon": -73.9840, "observer": "Alice Brown", "body_size": "Medium"},
        {"species_name": "Cardinalis cardinalis", "location": "Richmond, VA", "lat": 37.5407, "lon": -77.4360, "observer": "Bob Wilson", "migration_status": "Resident"}
    ]

    topic = 'observations'

    for s in sightings:
        print(f"Sending sighting: {s['species_name']} at {s['location']}")
        producer.send(topic, s)
        time.sleep(1) # Simulate delay

    producer.flush()
    producer.close()
    print("Finished producing mock sightings.")

if __name__ == "__main__":
    produce_mock_sightings()
