import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta, timezone
from minio import Minio
from minio.error import S3Error
from io import BytesIO

client = Minio(
    "127.0.0.1:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

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

def fetch_csv_from_minio(bucket_name, object_name):
    try:
        response = client.get_object(bucket_name, object_name)
        data = BytesIO(response.read()) 
        response.close()
        response.release_conn()
        print(f"Downloaded {object_name} from {bucket_name}")
        return data
    except S3Error as err:
        print(f"Failed to download {object_name}: {err}")
        return None

conn = psycopg2.connect(
    dbname="citybikes_data", user="postgres", password="postgres", host="localhost", port="5432"
)
cur = conn.cursor()

bucket_name = "citybikes-silver-layer"
latest_file = get_latest_silver_file(bucket_name)

if latest_file:
    csv_data = fetch_csv_from_minio(bucket_name, latest_file)
    
    if csv_data:
        df = pd.read_csv(csv_data)

        def insert_station_data():
            for index, row in df.iterrows():
                cur.execute("""
                    INSERT INTO dim_station (station_id, station_name, latitude, longitude, city_name)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (station_id) DO NOTHING
                """, (row['id'], row['name'], row['latitude'], row['longitude'], row['city_name']))
            conn.commit()
            print("Station data inserted into dim_station")

        def insert_time_data(timestamp):
            dt = datetime.fromisoformat(timestamp).replace(microsecond=0).astimezone(timezone(timedelta(hours=2)))
            
            cur.execute("""
                SELECT time_id FROM dim_time
                WHERE timestamp = %s
            """, (dt,))
            
            result = cur.fetchone()
            if result:
                return result[0]  
            else:
                cur.execute("""
                    INSERT INTO dim_time (timestamp, day, hour)
                    VALUES (%s, %s, %s) RETURNING time_id
                """, (dt, dt.date(), dt.hour))
                time_id = cur.fetchone()[0]
                conn.commit()
                return time_id


        def insert_fact_data():
            for index, row in df.iterrows():
                time_id = insert_time_data(row['timestamp'])
                
                cur.execute("""
                    SELECT 1 FROM fact_bike_availability
                    WHERE station_id = %s AND time_id = %s
                """, (row['id'], time_id))
                
                if cur.fetchone() is None:
                    cur.execute("""
                        INSERT INTO fact_bike_availability (station_id, time_id, free_bikes, empty_slots)
                        VALUES (%s, %s, %s, %s)
                    """, (row['id'], time_id, row['free_bikes'], row['empty_slots']))
                    conn.commit()
                else:
                    print(f"Data already exists for station {row['id']} at time {time_id}.")


        insert_station_data()
        insert_fact_data()

    cur.close()
    conn.close()
