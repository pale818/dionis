import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import os

def scrape():
    print("Starting species scraping...")
    
    # URL to scrape
    url = "https://aves.regoch.net"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["dionis"]
    collection = db["species"]
    
    # We expect a list or table of species
    # According to requirements, we need to extract species name (lat. names usually)
    # Since I don't have the exact HTML structure of the mock site, I'll look for generic patterns
    # and adapt if needed. Let's assume there are <li> or <tr> elements with species info.
    
    species_found = 0
    # Common pattern for mock sites: list items or table rows
    items = soup.find_all(['li', 'tr'])
    
    for item in items:
        # Looking for scientific names (often in <i> or descriptive text)
        text = item.get_text(strip=True)
        if text and len(text) > 3:
            # Simple deduplication by name
            if collection.find_one({"name": text}) is None:
                collection.insert_one({"name": text, "source": "aves.regoch.net"})
                species_found += 1
    
    print(f"Scraping complete. Found {species_found} new species.")
    client.close()

if __name__ == "__main__":
    scrape()