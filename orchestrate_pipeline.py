import os
from prefect import flow, task
import subprocess

# Define your tasks for each step in the pipeline

@task
def data_ingestion():
    print("Starting data ingestion...")
    subprocess.run(["citybikes_env/Scripts/python", "data_Ingestion/fetch_networks.py"], check=True)

@task
def data_processing():
    print("Starting data processing...")
    subprocess.run(["citybikes_env/Scripts/python", "data_Processing/processing.py"], check=True)

@task
def data_loading():
    print("Starting data loading...")
    subprocess.run(["citybikes_env/Scripts/python", "data_Loading/load.py"], check=True)

@task
def data_transforming():
    print("Starting data transformation...")
    subprocess.run(["citybikes_env/Scripts/python", "data_Transforming/transform.py"], check=True)

# Define the Prefect flow
@flow(name="CityBikes Data Pipeline")
def citybikes_pipeline():
    # Step 1: Data Ingestion
    data_ingestion()
    
    # Step 2: Data Processing
    data_processing()
    
    # Step 3: Data Loading
    data_loading()
    
    # Step 4: Data Transforming
    data_transforming()

if __name__ == "__main__":
    citybikes_pipeline()
