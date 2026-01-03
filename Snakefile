rule all:
    input:
        "data/backbone.done",
        "data/sightings.done",
        "data/audio.done"

rule scrape_species:
    output:
        "data/backbone.done"
    shell:
        "python scripts/scrape_species.py && python -c \"import os; os.makedirs('data', exist_ok=True); open('data/backbone.done', 'a').close()\""

rule consume_kafka:
    output:
        "data/sightings.done"
    shell:
        "python scripts/consume_kafka.py && python -c \"import os; os.makedirs('data', exist_ok=True); open('data/sightings.done', 'a').close()\""

rule audio_processing:
    output:
        "data/audio.done"
    shell:
        "python scripts/upload_and_classify.py && python -c \"import os; os.makedirs('data', exist_ok=True); open('data/audio.done', 'a').close()\""