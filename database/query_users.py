import psycopg2
import pandas as pd
from psycopg2 import sql
from os import getenv
from dotenv import load_dotenv

# Variable Definitions
load_dotenv()
DB_HOST = getenv("DB_HOST")
DB_PORT = getenv("DB_PORT")
DB_NAME = getenv("DB_NAME")
DB_USER = getenv("DB_USER")
DB_PASS = getenv("DB_PASS")


def query_users():
    cursor = None
    connection = None

    try:
        # Connect to the database
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS
        )

        # Create a cursor
        cursor = connection.cursor()

        # Define your SQL query (replace with your own query)
        query = sql.SQL("""
            SELECT *
            FROM user_registration
        """)

        # Execute the query
        cursor.execute(query)

        # Fetch all results into a Pandas DataFrame
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=columns)

        # Print or process the DataFrame
        print(df)
        print(df.columns)

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error querying users:", error)
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    query_users()
