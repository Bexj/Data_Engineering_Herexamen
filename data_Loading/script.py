import psycopg2
import pandas as pd
from datetime import datetime
from minio import Minio
from minio.error import S3Error

# Initialize MinIO client
client = Minio(
    "127.0.0.1:9000",  # Adjust to your MinIO instance
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Function to get the latest file from the silver layer bucket
def get_latest_silver_file(bucket_name):
    try:
        objects = client.list_objects(bucket_name, recursive=True)
        latest_file = None
        for obj in objects:
            if latest_file is None or obj.last_modified > latest_file.last_modified:
                latest_file = obj

        if latest_file:
            print(f"Latest file found: {latest_file.object_name}")
            return latest_file.object_name
        else:
            print("No files found in the silver layer.")
            return None
    except S3Error as err:
        print(f"Error listing objects: {err}")
        return None

# Download the CSV file from MinIO
def fetch_csv_from_minio(bucket_name, object_name, file_path):
    try:
        client.fget_object(bucket_name, object_name, file_path)
        print(f"Downloaded {object_name} from {bucket_name}")
    except S3Error as err:
        print(f"Failed to download {object_name}: {err}")

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="citybikes_data", user="postgres", password="postgres", host="localhost", port="5432"
)
cur = conn.cursor()

# Fetch the latest file from the silver layer bucket
bucket_name = "citybikes-silver-layer"
latest_file = get_latest_silver_file(bucket_name)

if latest_file:
    # Path to save the file locally
    file_path = "/tmp/cleaned_stations.csv"
    
    # Fetch the latest CSV file from MinIO
    fetch_csv_from_minio(bucket_name, latest_file, file_path)
    
    # Load cleaned data from the downloaded CSV file
    df = pd.read_csv(file_path)

    # Insert station data into dim_station
    def insert_station_data():
        for index, row in df.iterrows():
            cur.execute("""
                INSERT INTO dim_station (station_id, station_name, latitude, longitude, city_name)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (station_id) DO NOTHING
            """, (row['id'], row['name'], row['latitude'], row['longitude'], row['city_name']))
        conn.commit()
        print("Station data inserted into dim_station")

    # Insert time data into dim_time
    def insert_time_data(timestamp):
        dt = datetime.fromisoformat(timestamp).replace(microsecond=0)
        cur.execute("""
            INSERT INTO dim_time (timestamp, day, hour)
            VALUES (%s, %s, %s) RETURNING time_id
        """, (dt, dt.date(), dt.hour))
        time_id = cur.fetchone()[0]
        conn.commit()
        return time_id

    # Insert data into fact_bike_availability
    def insert_fact_data():
        for index, row in df.iterrows():
            # Insert time data and get time_id
            time_id = insert_time_data(row['timestamp'])
            
            # Insert bike availability data
            cur.execute("""
                INSERT INTO fact_bike_availability (station_id, time_id, free_bikes, empty_slots)
                VALUES (%s, %s, %s, %s)
            """, (row['id'], time_id, row['free_bikes'], row['empty_slots']))
        conn.commit()
        print("Bike availability data inserted into fact_bike_availability")

    # Run the insertion process
    insert_station_data()
    insert_fact_data()

    # Close connection
    cur.close()
    conn.close()
