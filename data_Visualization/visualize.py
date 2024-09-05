import os
import streamlit as st
import pandas as pd
import plotly.express as px
from minio import Minio
from io import BytesIO
from minio.error import S3Error

client = Minio(
    "127.0.0.1:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

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

def fetch_csv_from_minio_to_memory(bucket_name, object_name):
    try:
        response = client.get_object(bucket_name, object_name)
        data = BytesIO(response.read())
        response.close()
        response.release_conn()
        return pd.read_csv(data) 
    except S3Error as err:
        st.error(f"Failed to download {object_name}: {err}")
        return pd.DataFrame()  

@st.cache_data
def load_data():
    bucket_name = "citybikes-gold-layer" 
    latest_file = get_latest_gold_file(bucket_name)

    if latest_file:
        return fetch_csv_from_minio_to_memory(bucket_name, latest_file)
    else:
        st.error("No data found in the gold layer.")
        return pd.DataFrame()

def main():
    st.set_page_config(layout="wide")
    st.title("🚲 CityBikes Free Bikes Evolution Dashboard")

    if st.button("Refresh Data"):
        st.cache_data.clear()  
    
    data = load_data()  

    if not data.empty:
        city_options = data['city_name'].unique()
        selected_city = st.sidebar.selectbox("Select a city to filter by:", city_options, index=list(city_options).index("Bruxelles") if "Bruxelles" in city_options else 0)

        data['datetime'] = pd.to_datetime(data['timestamp'])

        filtered_data = data[data['city_name'] == selected_city].copy()

        min_date = filtered_data['datetime'].min()
        max_date = filtered_data['datetime'].max()
        start_date, end_date = st.sidebar.date_input("Select Date Range:", [min_date, max_date], min_value=min_date, max_value=max_date)

        filtered_data = filtered_data[(filtered_data['datetime'].dt.date >= start_date) & (filtered_data['datetime'].dt.date <= end_date)]

        if not filtered_data.empty:
            fig = px.line(filtered_data, x='datetime', y='total_free_bikes', markers=True,
                          title=f"Evolution of Free Bikes in {selected_city} Over Time",
                          labels={'datetime': 'Time', 'total_free_bikes': 'Number of Free Bikes'})

            fig.update_traces(line=dict(width=4, color='blue'), marker=dict(size=8))
            fig.update_layout(
                title={'x': 0.5, 'xanchor': 'center', 'font': {'size': 24, 'color': 'black'}},
                xaxis_title='Time',
                yaxis_title='Number of Free Bikes',
                xaxis=dict(showgrid=True, gridcolor='lightgray', gridwidth=0.5,
                           tickfont=dict(size=14, color='black'),
                           title_font=dict(size=18, color='black')),
                yaxis=dict(showgrid=True, gridcolor='lightgray', gridwidth=0.5,
                           tickfont=dict(size=14, color='black'),
                           title_font=dict(size=18, color='black')),
                margin=dict(l=0, r=0, t=40, b=40),
                hovermode="x unified",
                height=600,
                plot_bgcolor="white",
                paper_bgcolor="white",
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data available for the selected city and date range.")
    else:
        st.warning("No data available for visualization.")

if __name__ == "__main__":
    main()
