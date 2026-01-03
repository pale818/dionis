import os
import boto3
from botocore.client import Config
from pymongo import MongoClient
import time

def upload_and_classify():
    print("Starting audio processing (MinIO & Classification)...")
    
    # 1. Connect to MinIO
    # Note: Using localhost because we are outside Docker
    s3 = boto3.resource('s3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    bucket_name = 'bird-audio'
    bucket = s3.Bucket(bucket_name)
    
    # Ensure bucket exists
    if bucket.creation_date is None:
        print(f"Creating bucket: {bucket_name}")
        s3.create_bucket(Bucket=bucket_name)

    # 2. Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["dionis"]
    audio_collection = db["audio_sightings"]
    
    audio_dir = "data/audio"
    if not os.path.exists(audio_dir):
        print(f"Directory {audio_dir} not found.")
        return

    files = [f for f in os.listdir(audio_dir) if f.endswith('.wav')]
    print(f"Found {len(files)} audio files.")

    for filename in files:
        filepath = os.path.join(audio_dir, filename)
        
        # 3. Upload to MinIO
        print(f"Uploading {filename} to MinIO...")
        s3.meta.client.upload_file(filepath, bucket_name, filename)
        
        # 4. Mock Classification API
        # In a real scenario, we'd hit requests.post(api_url, data=file_stream)
        print(f"Requesting classification for {filename}...")
        time.sleep(1) # Simulate API latency
        
        # Mocking a response
        classification = {
            "file_name": filename,
            "s3_path": f"s3://{bucket_name}/{filename}",
            "species_name": "Turdus migratorius", # Mock result
            "confidence": 0.92,
            "location": "Forest Edge", # Metadata as per requirements
            "timestamp": time.time()
        }
        
        # 5. Save to MongoDB
        audio_collection.insert_one(classification)
        print(f"Classification saved for {filename}: {classification['species_name']} ({classification['confidence']})")

    print("Audio processing complete.")
    client.close()

if __name__ == "__main__":
    upload_and_classify()
