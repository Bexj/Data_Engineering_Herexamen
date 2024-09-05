import os
from prefect import flow, task
import subprocess


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

@task
def data_visualization():
    print("Starting data visualization (Streamlit)...")
    # Streamlit run command
    subprocess.run(["streamlit", "run", "data_Visualization/visualize.py"], check=True)

@flow(name="CityBikes Data Pipeline")
def citybikes_pipeline():
    data_ingestion()
    
    data_processing()
    
    data_loading()
    
    data_transforming()

    data_visualization()

if __name__ == "__main__":
    citybikes_pipeline()
