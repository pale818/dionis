import pymongo
from minio import Minio
from confluent_kafka.admin import AdminClient, NewTopic

SEARCH_FILTER = config.get("filter", "")



rule all:
    input:
        "data/csv/bird_statistics_report.csv",
        "data/visualizations/sightings_chart.png" 

rule run_kafka_producer:
    output:
        touch("data/.producer_done")
    script:
        "scripts/insert_to_kafka.py"

rule run_kafka_consumer:
    input:
        "data/.producer_done" 
    output:
        touch("data/.kafka_done") 
    script:
        "scripts/kafka_consumer.py"

rule run_audio:
    output:
        touch("data/.audio_done")
    script:
        "scripts/process_audio.py"

rule generate_report:
    input:
        audio = "data/.audio_done",
        kafka = "data/.kafka_done"
    output:
        "data/csv/bird_statistics_report.csv"
    shell:
        "python scripts/generate_report.py {SEARCH_FILTER}"

rule visualize_results:
    input:
        "data/csv/bird_statistics_report.csv"
    output:
        "data/visualizations/sightings_chart.png"
    script:
        "scripts/visualize_data.py"
 
rule clean:
    run:
        client = pymongo.MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")
        db = client["bird_db"]
        db.observations.delete_many({})
        db.classifications.delete_many({})
        print("MongoDB: Cleared observations and classifications.")

        s3 = Minio("localhost:9000", access_key="admin", secret_key="password123", secure=False)
        bucket = "bird-audio"
        if s3.bucket_exists(bucket):
            objects = s3.list_objects(bucket, recursive=True)
            for obj in objects:
                s3.remove_object(bucket, obj.object_name)
            print(f"MinIO: Cleared bucket '{bucket}'.")

        admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
        topic_name = "bird-observations"
        try:
            fs = admin_client.delete_topics([topic_name])
            for topic, f in fs.items():
                f.result() 
            print(f"Kafka: Topic '{topic_name}' deleted.")
        except Exception as e:
            print(f"Kafka: Topic deletion skipped or failed: {e}")

        shell("rm -f data/.*_done")
        shell("rm -f data/csv/bird_statistics_report.csv")
        shell("rm -f data/visualizations/sightings_chart.png")
        print("Filesystem: Removed marker files and report.")