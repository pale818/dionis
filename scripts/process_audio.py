import os
import json
import random
from datetime import datetime
from minio import Minio
from pymongo import MongoClient

# --- CONFIGURATION ---
# Folder from your image
SOURCE_DIR = "data/audio" 
# Folder position as per PDF simplification
FOLDER_LAT = 45.8150 
FOLDER_LNG = 15.9819

# Connections
minio_client = Minio("localhost:9000", access_key="admin", secret_key="password123", secure=False)
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["bird_db"]
classifications_col = db["classifications"]

def mock_classification_api(filename):
   

    base = os.path.splitext(filename)[0]   
    species = base.replace("_", " ").strip()

    return {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "identified_species": species,
        "confidence_score": 0.95,
        "processed_file": filename
    }

def mock_classification_api_random(filename):

    possible_birds = ["Guttera pucherani", "Agelastes niger", "Acryllium vulturinum"]
    return {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "identified_species": random.choice(possible_birds),
        "confidence_score": round(random.uniform(0.75, 0.99), 2),
        "processed_file": filename
    }

def run_audio_pipeline():
    # Ensure MinIO Bucket exists
    bucket_name = "bird-audio"
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    print(f"Starting audio processing from {SOURCE_DIR}...")

    for filename in os.listdir(SOURCE_DIR):
        if filename.endswith((".wav", ".mp3")):
            file_path = os.path.join(SOURCE_DIR, filename)
            
            # Upload Audio File to MinIO 
            minio_client.fput_object(bucket_name, filename, file_path)
            file_url = f"http://localhost:9000/{bucket_name}/{filename}"
            print(f"Uploaded {filename} to MinIO.")

            # Request Classification 
            api_response = mock_classification_api(filename)
            
            # Store Request Log in MinIO 
            log_filename = f"logs/{filename}_log.json"
            log_data = json.dumps(api_response).encode('utf-8')
            from io import BytesIO
            minio_client.put_object(bucket_name, log_filename, BytesIO(log_data), len(log_data))

            # Store Results and Metadata in MongoDB 
            doc = {
                "filename": filename,
                "minio_url": file_url,
                "latitude": FOLDER_LAT,
                "longitude": FOLDER_LNG,
                "classification": {
                    "species": api_response["identified_species"],
                    "confidence": api_response["confidence_score"]
                },
                "processed_at": datetime.now()
            }
            classifications_col.insert_one(doc)
            print(f"Classified {filename} as {api_response['identified_species']} ({api_response['confidence_score']})")

    print("\nAudio Processing Step Complete!")

if __name__ == "__main__":
    run_audio_pipeline()