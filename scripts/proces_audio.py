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
mongo_client = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")
db = mongo_client["bird_db"]
classifications_col = db["classifications"]

def mock_classification_api(filename):
    """
    Simulates the publicly available classification model ().
    Returns bird info and confidence score.
    """
    # Using real names from your scraped backbone (Aves Portal)
    possible_birds = ["Guttera pucherani", "Agelastes niger", "Acryllium vulturinum"]
    return {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "identified_species": random.choice(possible_birds),
        "confidence_score": round(random.uniform(0.75, 0.99), 2),
        "processed_file": filename
    }

def run_audio_pipeline():
    # 1. Ensure MinIO Bucket exists
    bucket_name = "bird-audio"
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    print(f"Starting audio processing from {SOURCE_DIR}...")

    for filename in os.listdir(SOURCE_DIR):
        if filename.endswith((".wav", ".mp3")):
            file_path = os.path.join(SOURCE_DIR, filename)
            
            # 2. Upload Audio File to MinIO (LO2 Minimal)
            minio_client.fput_object(bucket_name, filename, file_path)
            file_url = f"http://localhost:9000/{bucket_name}/{filename}"
            print(f"Uploaded {filename} to MinIO.")

            # 3. Request Classification (LO2 Desired)
            api_response = mock_classification_api(filename)
            
            # 4. Store Request Log in MinIO (LO2 Desired)
            log_filename = f"logs/{filename}_log.json"
            log_data = json.dumps(api_response).encode('utf-8')
            # Using a temporary file or BytesIO for the log upload
            from io import BytesIO
            minio_client.put_object(bucket_name, log_filename, BytesIO(log_data), len(log_data))

            # 5. Store Results and Metadata in MongoDB (LO3 Minimal)
            # Associating file with metadata (location, filename)
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