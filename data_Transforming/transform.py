import psycopg2
import pandas as pd
from minio import Minio
from minio.error import S3Error
from datetime import datetime

# Initialize MinIO client
client = Minio(
    "127.0.0.1:9000",  # Adjust to your MinIO instance
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="citybikes_data", user="postgres", password="postgres", host="localhost", port="5432"
)
cur = conn.cursor()

# Query to aggregate free bikes per city over time
query = """
    SELECT 
        ds.city_name,
        dt.day,
        dt.hour,
        SUM(fb.free_bikes) AS total_free_bikes
    FROM fact_bike_availability fb
    JOIN dim_station ds ON fb.station_id = ds.station_id
    JOIN dim_time dt ON fb.time_id = dt.time_id
    GROUP BY ds.city_name, dt.day, dt.hour
    ORDER BY ds.city_name, dt.day, dt.hour;
"""

cur.execute(query)
rows = cur.fetchall()

# Convert query result to a DataFrame
df = pd.DataFrame(rows, columns=["city_name", "day", "hour", "total_free_bikes"])

# Close the connection
cur.close()
conn.close()

# Save the aggregated data to a CSV file
file_path = f"/tmp/aggregated_free_bikes_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
df.to_csv(file_path, index=False)
print(f"Aggregated data saved to {file_path}")

# Upload the CSV to the gold layer in MinIO
def upload_to_minio(bucket_name, file_path):
    file_name = file_path.split("/")[-1]
    try:
        client.fput_object(bucket_name, file_name, file_path)
        print(f"Uploaded {file_name} to {bucket_name}")
    except S3Error as err:
        print(f"Failed to upload {file_name}: {err}")

# Specify the gold layer bucket name
gold_bucket = "citybikes-gold-layer"
upload_to_minio(gold_bucket, file_path)
