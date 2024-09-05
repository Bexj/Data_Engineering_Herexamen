from datetime import datetime
import polars as pl
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

# List files in the bronze layer and find the latest one
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

# Fetch the latest file from the bronze layer
def fetch_bronze_data(bucket_name, file_name):
    try:
        file_path = f"/tmp/{file_name}"
        client.fget_object(bucket_name, file_name, file_path)
        print(f"Downloaded {file_name} from {bucket_name}")
        return file_path
    except S3Error as err:
        print(f"Failed to fetch {file_name}: {err}")
        return None

# Load the CSV into a Polars DataFrame
def load_data(file_path):
    try:
        df = pl.read_csv(file_path)
        print(f"Data loaded from {file_path}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

# Deduplicate and validate data
def process_data(df):
    # Remove 'Z' from the 'timestamp' column and cast to string
    df = df.with_columns([
        pl.col("timestamp").str.replace("Z$", "").alias("timestamp"),  # Remove 'Z' at the end of the timestamp
        pl.col("free_bikes").cast(pl.Int64, strict=False),  # Cast to integer (with relaxed casting)
        pl.col("empty_slots").cast(pl.Int64, strict=False), # Cast to integer (with relaxed casting)
        pl.col("latitude").cast(pl.Float64, strict=False),  # Cast to float
        pl.col("longitude").cast(pl.Float64, strict=False)  # Cast to float
    ])

    # Deduplicate based on station ID and timestamp (keeping the most recent record)
    df = df.sort(by="timestamp", descending=True).unique(subset=["id", "timestamp"])
    
    # Validation - drop rows with missing critical values
    df = df.drop_nulls(subset=["free_bikes", "empty_slots", "timestamp", "city_name"])
    
    # Filter out rows with negative or nonsensical values
    df = df.filter(
        (pl.col("free_bikes") >= 0) & 
        (pl.col("empty_slots") >= 0) &
        (pl.col("timestamp").is_not_null())
    )

    print("Data processed: deduplicated and validated")
    return df


# Save the cleaned data to a CSV and upload it to the silver layer
def save_and_upload(df, bucket_name, file_name):
    silver_file_path = f"/tmp/{file_name}"
    df.write_csv(silver_file_path)
    print(f"Cleaned data saved to {silver_file_path}")

    try:
        client.fput_object(bucket_name, file_name, silver_file_path)
        print(f"Uploaded {file_name} to {bucket_name}")
    except S3Error as err:
        print(f"Failed to upload {file_name}: {err}")

if __name__ == "__main__":
    # Step 1: Find the latest file in the bronze layer
    bronze_bucket = "citybikes-bronze-layer"
    bronze_file = get_latest_bronze_file(bronze_bucket)

    if bronze_file:
        # Step 2: Fetch the latest file
        bronze_file_path = fetch_bronze_data(bronze_bucket, bronze_file)

        if bronze_file_path:
            # Step 3: Load the data into Polars
            df = load_data(bronze_file_path)

            if df is not None:
                # Step 4: Process the data (deduplicate and validate)
                cleaned_df = process_data(df)

                # Step 5: Save and upload to the silver layer
                silver_bucket = "citybikes-silver-layer"
                silver_file = f"cleaned_stations_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
                save_and_upload(cleaned_df, silver_bucket, silver_file)
