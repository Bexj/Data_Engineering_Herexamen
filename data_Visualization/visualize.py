import streamlit as st
import pandas as pd
import plotly.express as px
from minio import Minio
from io import BytesIO
from minio.error import S3Error

# Initialize MinIO client
client = Minio(
    "127.0.0.1:9000",  # Adjust to your MinIO instance
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Function to fetch the latest file from the gold layer bucket in MinIO
def get_latest_gold_file(bucket_name):
    try:
        objects = client.list_objects(bucket_name, recursive=True)
        latest_file = None
        for obj in objects:
            if latest_file is None or obj.last_modified > latest_file.last_modified:
                latest_file = obj
        return latest_file.object_name if latest_file else None
    except S3Error as err:
        st.error(f"Error listing objects: {err}")
        return None

# Download the CSV file from MinIO directly into memory
def fetch_csv_from_minio_to_memory(bucket_name, object_name):
    try:
        response = client.get_object(bucket_name, object_name)
        data = BytesIO(response.read())  # Read the data into a BytesIO object
        response.close()
        response.release_conn()
        return pd.read_csv(data)  # Read the data into a Pandas DataFrame
    except S3Error as err:
        st.error(f"Failed to download {object_name}: {err}")
        return pd.DataFrame()  # Return an empty DataFrame in case of failure

# Load the data into a Pandas DataFrame
@st.cache_data
def load_data():
    bucket_name = "citybikes-gold-layer"  # Adjust the gold bucket name
    latest_file = get_latest_gold_file(bucket_name)

    if latest_file:
        return fetch_csv_from_minio_to_memory(bucket_name, latest_file)
    else:
        st.error("No data found in the gold layer.")
        return pd.DataFrame()

# Main function to build the Streamlit dashboard
def main():
    st.set_page_config(layout="wide")  # Set the page layout to wide mode for better use of space
    st.title("🚲 CityBikes Free Bikes Evolution Dashboard")

    # Load the data (historical + new)
    data = load_data()

    if not data.empty:
        # Sidebar for city selection and date range filtering
        city_options = data['city_name'].unique()
        selected_city = st.sidebar.selectbox("Select a city to filter by:", city_options)

        # Convert day and hour to datetime
        data['datetime'] = pd.to_datetime(data['day'] + ' ' + data['hour'].astype(str) + ':00')

        # Filter the data by the selected city
        filtered_data = data[data['city_name'] == selected_city].copy()

        # Add a date range filter
        min_date = filtered_data['datetime'].min()
        max_date = filtered_data['datetime'].max()
        start_date, end_date = st.sidebar.date_input("Select Date Range:", [min_date, max_date], min_value=min_date, max_value=max_date)

        # Apply date range filter
        filtered_data = filtered_data[(filtered_data['datetime'].dt.date >= start_date) & (filtered_data['datetime'].dt.date <= end_date)]

        if not filtered_data.empty:
            # Create the line chart using Plotly for interactivity
            fig = px.line(filtered_data, x='datetime', y='total_free_bikes', markers=True,
                          title=f"Evolution of Free Bikes in {selected_city} Over Time",
                          labels={'datetime': 'Time', 'total_free_bikes': 'Number of Free Bikes'})

            # Customize the layout for better visuals
            fig.update_traces(line=dict(width=4, color='blue'), marker=dict(size=8))  # Thicker line, larger markers
            fig.update_layout(
                title={'x': 0.5, 'xanchor': 'center', 'font': {'size': 24, 'color': 'black'}},  # Centered, large title
                xaxis_title='Time',
                yaxis_title='Number of Free Bikes',
                xaxis=dict(showgrid=True, gridcolor='lightgray', gridwidth=0.5,  # Light gridlines
                           tickfont=dict(size=14, color='black'),  # Bigger, darker tick marks
                           title_font=dict(size=18, color='black')),  # Larger x-axis label
                yaxis=dict(showgrid=True, gridcolor='lightgray', gridwidth=0.5,
                           tickfont=dict(size=14, color='black'),  # Bigger, darker tick marks
                           title_font=dict(size=18, color='black')),  # Larger y-axis label
                margin=dict(l=0, r=0, t=40, b=40),  # Minimize margins
                hovermode="x unified",  # Unified hover mode for better tooltips
                height=600,  # Height of the chart
                plot_bgcolor="white",  # Background color for a cleaner look
                paper_bgcolor="white",  # Remove black background
            )

            # Display the plotly chart
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data available for the selected city and date range.")
    else:
        st.warning("No data available for visualization.")

# Streamlit entry point
if __name__ == "__main__":
    main()
