import psycopg2
from psycopg2 import sql, OperationalError

def create_tables():
    try:
        # Connect to the citybikes_data database on the Dockerized TimeScaleDB instance
        conn = psycopg2.connect(
            dbname="citybikes_data", user="postgres", password="postgres", host="localhost", port="5432"
        )
        cur = conn.cursor()

        # Create the station dimension table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_station (
                station_id VARCHAR PRIMARY KEY,
                station_name VARCHAR NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                city_name VARCHAR NOT NULL
            )
        """)

        # Create the time dimension table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_time (
                time_id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                day DATE NOT NULL,
                hour INTEGER NOT NULL
            )
        """)

        # Create the fact table for bike availability
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_bike_availability (
                station_id VARCHAR REFERENCES dim_station(station_id),
                time_id INTEGER REFERENCES dim_time(time_id),
                free_bikes INTEGER NOT NULL,
                empty_slots INTEGER NOT NULL,
                PRIMARY KEY (station_id, time_id)
            )
        """)

        conn.commit()
        print("Tables created successfully")

        cur.close()
        conn.close()

    except OperationalError as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    create_tables()
