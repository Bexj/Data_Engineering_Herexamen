import os
import psycopg2
import pandas as pd
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from datetime import datetime

client = Minio(
    "127.0.0.1:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"), host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT")
)
cur = conn.cursor()

query = """
    SELECT 
        ds.city_name,
        dt.timestamp,
        SUM(fb.free_bikes) AS total_free_bikes
    FROM fact_bike_availability fb
    JOIN dim_station ds ON fb.station_id = ds.station_id
    JOIN dim_time dt ON fb.time_id = dt.time_id
    GROUP BY ds.city_name, dt.timestamp
    ORDER BY ds.city_name, dt.timestamp;
"""

cur.execute(query)
rows = cur.fetchall()

df = pd.DataFrame(rows, columns=["city_name", "timestamp", "total_free_bikes"])

cur.close()
conn.close()

def upload_to_minio_in_memory(df, bucket_name, file_name):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False) 
    csv_buffer.seek(0) 

    try:
        client.put_object(bucket_name, file_name, data=csv_buffer, length=csv_buffer.getbuffer().nbytes)
        print(f"Uploaded {file_name} to {bucket_name}")
    except S3Error as err:
        print(f"Failed to upload {file_name}: {err}")

file_name = f"aggregated_free_bikes_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

gold_bucket = "citybikes-gold-layer"
upload_to_minio_in_memory(df, gold_bucket, file_name)
