import requests
import pandas as pd
import time
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import os

# Initialize MinIO client
client = Minio(
    "127.0.0.1:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Define cities of interest in Belgium
BELGIAN_CITIES = ["Bruxelles", "Antwerpen", "Gent", "Namur"]

def fetch_network_data():
    url = "http://api.citybik.es/v2/networks?fields=id,name,location,href"
    response = requests.get(url)
    response.raise_for_status()

    networks = response.json().get("networks", [])
    
    # Filter networks for only those in Belgian cities
    filtered_networks = [
        network for network in networks 
        if network['location']['city'] in BELGIAN_CITIES
    ]
    return filtered_networks

def fetch_station_data(network_id, network_name, city_name, backoff_time=1):
    url = f"http://api.citybik.es/v2/networks/{network_id}"
    
    while True:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                stations = response.json().get("network", {}).get("stations", [])
                # Add the city name to each station entry
                for station in stations:
                    station["network_name"] = network_name
                    station["city_name"] = city_name  # Include city name in station data
                return stations
            elif response.status_code == 429:
                print(f"Rate limit hit for {network_name}. Backing off for {backoff_time} seconds...")
                time.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
            else:
                raise Exception(f"Failed to fetch stations for {network_name}: {response.status_code}")
        except requests.RequestException as e:
            print(f"Error fetching stations for {network_name}: {e}")
            time.sleep(backoff_time)
            backoff_time *= 2

def consolidate_station_data(networks):
    all_stations = []  # List to hold all station data

    for network in networks:
        network_id = network["id"]
        network_name = network["name"]
        city_name = network["location"]["city"]  # Extract city name

        print(f"Fetching stations for {network_name} (ID: {network_id}) in {city_name}")
        
        stations = fetch_station_data(network_id, network_name, city_name)
        if stations:
            all_stations.extend(stations)  # Append to the master list
        else:
            print(f"No station data found for {network_name}.")
        
        time.sleep(1)  # Controlled delay to avoid rate-limiting

    return pd.DataFrame(all_stations)

def save_to_csv(df):
    # Remove unnecessary columns like 'extra' if they exist
    columns_to_keep = ['id', 'name', 'latitude', 'longitude', 'timestamp', 'free_bikes', 'empty_slots', 'network_name', 'city_name']
    df = df[columns_to_keep]  # Filter the DataFrame to keep only the relevant columns
    
    folder_path = "station_data"
    os.makedirs(folder_path, exist_ok=True)
    filename = f"{folder_path}/consolidated_stations_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    
    df.to_csv(filename, index=False)
    print(f"Consolidated station data saved to {filename}")
    return filename

def upload_to_minio(bucket_name, file_path, file_name):
    try:
        client.fput_object(bucket_name, file_name, file_path)
        print(f"Uploaded {file_name} to {bucket_name}")
    except S3Error as err:
        print(f"Failed to upload {file_name}: {err}")

if __name__ == "__main__":
    # Fetch network data
    networks = fetch_network_data()
    
    # Consolidate all station data into a single DataFrame
    consolidated_station_data = consolidate_station_data(networks)
    
    # Save the consolidated station data to a single CSV file
    csv_filename = save_to_csv(consolidated_station_data)
    
    # Upload the single CSV file to MinIO
    upload_to_minio("citybikes-bronze-layer", csv_filename, os.path.basename(csv_filename))
