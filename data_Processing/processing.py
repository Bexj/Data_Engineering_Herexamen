from datetime import datetime
import polars as pl
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


def get_latest_bronze_file(bucket_name):
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
            print("No files found in the bronze layer.")
            return None
    except S3Error as err:
        print(f"Error listing objects: {err}")
        return None

def fetch_bronze_data(bucket_name, file_name):
    try:
        response = client.get_object(bucket_name, file_name)
        data = BytesIO(response.read())  
        response.close()
        response.release_conn()
        print(f"Downloaded {file_name} from {bucket_name}")
        return data
    except S3Error as err:
        print(f"Failed to fetch {file_name}: {err}")
        return None

def load_data(data):
    try:
        df = pl.read_csv(data)
        print(f"Data loaded from memory")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def process_data(df):
    df = df.with_columns([
        pl.col("timestamp").str.replace("Z$", "").alias("timestamp"),  
        pl.col("free_bikes").cast(pl.Int64, strict=False),  
        pl.col("empty_slots").cast(pl.Int64, strict=False), 
        pl.col("latitude").cast(pl.Float64, strict=False),  
        pl.col("longitude").cast(pl.Float64, strict=False)  
    ])

    df = df.sort(by="timestamp", descending=True).unique(subset=["id", "timestamp"])
    df = df.drop_nulls(subset=["free_bikes", "empty_slots", "timestamp", "city_name"])
    df = df.filter(
        (pl.col("free_bikes") >= 0) & 
        (pl.col("empty_slots") >= 0) &
        (pl.col("timestamp").is_not_null())
    )

    print("Data processed: deduplicated and validated")
    return df

def save_and_upload(df, bucket_name, file_name):
    csv_buffer = BytesIO()  
    df.write_csv(csv_buffer)
    csv_buffer.seek(0)  
    
    try:
        client.put_object(bucket_name, file_name, data=csv_buffer, length=csv_buffer.getbuffer().nbytes)
        print(f"Uploaded {file_name} to {bucket_name}")
    except S3Error as err:
        print(f"Failed to upload {file_name}: {err}")

if __name__ == "__main__":
    bronze_bucket = "citybikes-bronze-layer"
    bronze_file = get_latest_bronze_file(bronze_bucket)

    if bronze_file:
        bronze_data = fetch_bronze_data(bronze_bucket, bronze_file)

        if bronze_data:
            df = load_data(bronze_data)

            if df is not None:
                cleaned_df = process_data(df)

                silver_bucket = "citybikes-silver-layer"
                silver_file = f"cleaned_stations_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
                save_and_upload(cleaned_df, silver_bucket, silver_file)
