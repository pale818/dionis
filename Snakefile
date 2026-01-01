rule all:
    input:
        "data/backbone.done"

rule scrape_species:
    output:
        "data/backbone.done"
    shell:
        "python scripts/scrape_species.py && python -c \"import os; os.makedirs('data', exist_ok=True); open('data/backbone.done', 'a').close()\""