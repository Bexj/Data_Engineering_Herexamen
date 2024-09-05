import psycopg2
from psycopg2 import sql, OperationalError

def create_database():
    try:
        # Connect to the default Postgres database
        conn = psycopg2.connect(
            dbname="citybikes_data", user="postgres", password="postgres", host="localhost", port="5432"
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Check if 'citybikes_data' database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'citybikes_data'")
        exists = cur.fetchone()

        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE citybikes_data"))
            print("Database 'citybikes_data' created.")
        else:
            print("Database 'citybikes_data' already exists.")
        
        cur.close()
        conn.close()

    except OperationalError as e:
        print(f"Error: {e}")
        if conn:
            conn.close()

if __name__ == "__main__":
    create_database()
