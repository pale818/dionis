import requests
from pymongo import MongoClient
import sys

def scrape_birds():
    # Configuration
    JSON_URL = "https://aves.regoch.net/aves.json"
    MONGO_URI = "mongodb://admin:password@localhost:27017/?authSource=admin"
    DB_NAME = "bird_db"
    COLLECTION_NAME = "species"

    print(f"Connecting to MongoDB...")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        client.server_info()
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        sys.exit(1)

    print(f"Fetching data directly from {JSON_URL}...")
    try:
        response = requests.get(JSON_URL)
        response.raise_for_status()
        bird_data_list = response.json() # This parses the aves.json automatically
    except Exception as e:
        print(f"Error fetching data: {e}")
        sys.exit(1)

    birds_inserted = 0
    birds_skipped = 0

    for sp in bird_data_list:
        # Use scientificName or canonicalName as shown in the script tags you provided
        scientific_name = sp.get('scientificName')
        canonical_name = sp.get('canonicalName')
        
        if not canonical_name:
            continue

        # Check for duplicates using canonical_name 
        existing = collection.find_one({"latin_name": canonical_name})
        
        if existing:
            birds_skipped += 1
        else:
            bird_doc = {
                "common_name": scientific_name, 
                "latin_name": canonical_name,
                "rank": sp.get('rank'),
                "family": sp.get('family'),
                "order": sp.get('order'),
                "key": sp.get('key') # Useful for linking later
            }
            collection.insert_one(bird_doc)
            birds_inserted += 1
            print(f"Inserted: {canonical_name}")

    print(f"\nScraping complete! Inserted: {birds_inserted}, Skipped: {birds_skipped}")

if __name__ == "__main__":
    scrape_birds()