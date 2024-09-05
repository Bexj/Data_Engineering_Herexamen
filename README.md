# CityBikes Data Engineering Project

**Project Overview**
This project implements an end-to-end data pipeline for ingesting, processing, storing, and visualizing bike-sharing data from the CityBikes API. The goal is to track and analyze bike availability in different cities across Belgium, leveraging modern data engineering tools such as MinIO, TimescaleDB, Prefect, and Polars.

**Project Architecture**
The pipeline follows a clear architecture where data flows through ingestion, processing, transformation, and visualization stages, orchestrated by Prefect. Here's a brief breakdown:

* CityBikes API - The source of real-time bike-sharing data.
* MinIO - Acts as the data lake, storing raw, processed, and aggregated data.
* Bronze Layer - Raw data from the CityBikes API.
* Silver Layer - Cleaned and processed data.
* Gold Layer - Aggregated data ready for analysis and visualization.
* TimescaleDB - The data warehouse where processed data is stored for analysis.
* Prefect - Orchestrates the entire workflow, ensuring smooth transitions between pipeline stages.
* Streamlit - Provides a dashboard for visualizing bike availability over time.
* Docker - TimeScaleDB is containerized for easy deployment.
* Python - Main programming language for data pipeline scripts.
* Polars - Fast data processing library for handling large datasets.
* MinIO - Object storage used as a data lake.
* TimescaleDB - A PostgreSQL-based time-series database for data warehousing.
* Prefect - Orchestrates the pipeline’s workflow, ensuring tasks run in the correct order.
* Streamlit - Frontend visualization tool for displaying bike data interactively.

## Setting up the Application

1. **Create a Python Environment:**

* Set up a dedicated Python environment for running the application. This ensures that all necessary libraries and dependencies are isolated and managed correctly.

2. **Install Required Libraries:**

* Import all required libraries within this environment. The specific libraries and their versions are provided in the requirements.txt file.

3. **Set Up MinIO:**

* Install MinIO on your local machine or server. MinIO is an object storage server that can be used as a data lake for storing raw, processed, and aggregated data. Create a .env file in the root directory and folow the example in .env.example to set up the MinIO credentials.

4. **Set Up TimescaleDB:**

* Install TimescaleDB on your local machine or server. TimescaleDB is a PostgreSQL-based time-series database that will store the processed data for analysis. Folow the example in .env.example to set up the TimescaleDB credentials.

* You can use the files in the Database_setup folder to set up the database and tables in TimescaleDB. You can also clear the data from the tables using the clear_db_data.py file.

5. **Run the orchestrate_pipeline.py file**

* Run the orchestrate_pipeline.py file to start the Prefect flow. This script will orchestrate the entire workflow, ensuring smooth transitions between pipeline stages. It also runs the Streamlit dashboard for visualizing bike availability over time.

## Side note

* I didn't have time left to package the application using Docker, i only used Docker to run the TimescaleDB container. I worked very hard on this and hope that this isn't a huge issue. Everything worked perfectly on my pc as you can see in the screenshots in the project report. I actually liked making this weirdly enough, when everything started working it was very satisfying. I hope you like it too.

Also i see now that everything is still in folders, which was not how it needed to be, i saw it too late... 