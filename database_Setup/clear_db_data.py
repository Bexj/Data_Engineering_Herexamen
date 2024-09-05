import psycopg2

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="citybikes_data", user="postgres", password="postgres", host="localhost", port="5432"
)
cur = conn.cursor()

# Function to truncate all tables
def clear_database():
    try:
        # Disable foreign key checks temporarily to avoid issues with truncating tables with relationships
        cur.execute("SET session_replication_role = 'replica';")
        
        # Truncate the fact table first, then the dimension tables
        cur.execute("TRUNCATE TABLE fact_bike_availability RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE dim_time RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE dim_station RESTART IDENTITY CASCADE;")

        # Re-enable foreign key checks
        cur.execute("SET session_replication_role = 'origin';")

        conn.commit()
        print("All data cleared from the database.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()

# Run the clear database function
clear_database()
