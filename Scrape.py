import requests
import json
import time
import sqlite3
from datetime import datetime

class CityBikesAPI:
    BASE_URL = "http://api.citybik.es/v2/networks"

    def get_network_details(self, network_id):
        """Fetch details for a specific network."""
        url = f"{self.BASE_URL}/{network_id}"
        response = requests.get(url)
        return response.json()["network"]

def setup_database():
    """Set up SQLite database to store the data."""
    conn = sqlite3.connect('bike_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS bike_availability
                 (city TEXT, station_id TEXT, free_bikes INTEGER, timestamp DATETIME)''')
    conn.commit()
    return conn

def ingest_data(api, cities, conn):
    """Ingest data for specified cities and store in database."""
    for city, network_id in cities.items():
        network_details = api.get_network_details(network_id)
        timestamp = datetime.now().isoformat()
        
        for station in network_details['stations']:
            conn.execute('''INSERT INTO bike_availability 
                            (city, station_id, free_bikes, timestamp) 
                            VALUES (?, ?, ?, ?)''',
                         (city, station['id'], station['free_bikes'], timestamp))
    
    conn.commit()

def main():
    api = CityBikesAPI()
    conn = setup_database()

    # Select a few major cities
    cities = {
        "New York": "citi-bike-nyc",
        "London": "santander-cycles",
        "Paris": "velib",
        "Tokyo": "docomo-cycle-tokyo"
    }

    try:
        while True:
            print(f"Ingesting data at {datetime.now().isoformat()}")
            ingest_data(api, cities, conn)
            time.sleep(3600)  # Wait for 1 hour before next ingestion
    except KeyboardInterrupt:
        print("Data ingestion stopped.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()