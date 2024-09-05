import requests
import pandas as pd
import time
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os

client = Minio(
    "127.0.0.1:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

BELGIAN_CITIES = ["Bruxelles", "Antwerpen", "Gent", "Namur"]

def fetch_network_data():
    url = "http://api.citybik.es/v2/networks?fields=id,name,location,href"
    response = requests.get(url)
    response.raise_for_status()

    networks = response.json().get("networks", [])
    
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

                for station in stations:
                    station["network_name"] = network_name
                    station["city_name"] = city_name  
                return stations
            elif response.status_code == 429:
                print(f"Rate limit hit for {network_name}. Backing off for {backoff_time} seconds...")
                time.sleep(backoff_time)
                backoff_time *= 2  
            else:
                raise Exception(f"Failed to fetch stations for {network_name}: {response.status_code}")
        except requests.RequestException as e:
            print(f"Error fetching stations for {network_name}: {e}")
            time.sleep(backoff_time)
            backoff_time *= 2

def consolidate_station_data(networks):
    all_stations = []  

    for network in networks:
        network_id = network["id"]
        network_name = network["name"]
        city_name = network["location"]["city"]  

        print(f"Fetching stations for {network_name} (ID: {network_id}) in {city_name}")
        
        stations = fetch_station_data(network_id, network_name, city_name)
        if stations:
            all_stations.extend(stations)  
        else:
            print(f"No station data found for {network_name}.")
        
        time.sleep(1)  

    return pd.DataFrame(all_stations)

def upload_to_minio(df, bucket_name, file_name):
    try:
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  
        
        client.put_object(bucket_name, file_name, data=csv_buffer, length=csv_buffer.getbuffer().nbytes)
        print(f"Uploaded {file_name} to {bucket_name}")
    except S3Error as err:
        print(f"Failed to upload {file_name}: {err}")

if __name__ == "__main__":
    networks = fetch_network_data()
    
    consolidated_station_data = consolidate_station_data(networks)
    
    file_name = f"consolidated_stations_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    
    upload_to_minio(consolidated_station_data, "citybikes-bronze-layer", file_name)
