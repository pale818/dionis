import csv
import sys
from pymongo import MongoClient
from thefuzz import process

# --- CONFIGURATION ---
mongo_client = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")
db = mongo_client["bird_db"]

def generate_report(search_query=None):
    # 1. Fetch species backbone
    all_species = list(db.species.find())
    species_names = [s["latin_name"] for s in all_species]

    # 2. Fuzzy Filtering (Optional parameter)
    filtered_names = species_names
    if search_query:
        matches = process.extract(search_query, species_names, limit=10)
        filtered_names = [m[0] for m in matches if m[1] > 60]

    report_data = []

    for latin_name in filtered_names:
        species_info = next(s for s in all_species if s["latin_name"] == latin_name)
        
        # Count sightings from both sources
        k_count = db.observations.count_documents({"taxonomy_code": latin_name})
        a_count = db.classifications.count_documents({"classification.species": latin_name})
        total_sightings = k_count + a_count

        # 3. Handle "Relevant Observational Data" (LO3 Requirement)
        # We fetch all unique observations for this bird to collect their traits
        observations = list(db.observations.find({"taxonomy_code": latin_name}))
        
        traits = []
        for obs in observations:
            obs_details = obs.get("observation_data", {})
            # Convert dictionary {'habitat': 'Forest'} to string "habitat: Forest"
            for key, value in obs_details.items():
                trait_str = f"{key}: {value}"
                if trait_str not in traits:
                    traits.append(trait_str)
        
        additional_info = " | ".join(traits) if traits else "No extra data"

        report_data.append({
            "Species Name": latin_name,
            "Common Name": species_info.get("common_name", "N/A"),
            "Total Sightings": total_sightings,
            "Family": species_info.get("family", "N/A"),
            "Additional Info": additional_info
        })

    # 4. Export to CSV
    keys = ["Species Name", "Common Name", "Total Sightings", "Family", "Additional Info"]
    with open("data/csv/bird_statistics_report.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(report_data)

    print(f"Report generated successfully. Found {len(report_data)} matches.")

if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else None
    generate_report(query)