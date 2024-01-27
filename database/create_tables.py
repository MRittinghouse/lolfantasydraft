import psycopg2
from os import getenv
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve database configuration from environment variables
DB_HOST = getenv("DB_HOST")
DB_PORT = getenv("DB_PORT")
DB_NAME = getenv("DB_NAME")
DB_USER = getenv("DB_USER")
DB_PASS = getenv("DB_PASS")

# Define the SQL statement to create the user registration table
create_user_table_sql = """
CREATE TABLE IF NOT EXISTS user_registration (
    user_id SERIAL PRIMARY KEY,
    discord_id VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    channel_id VARCHAR(255) NOT NULL,
    registration_date TIMESTAMP DEFAULT current_timestamp
);
"""


def create_tables():
    # Initialize cursor and conn as None
    cursor = None
    connection = None

    try:
        # Connect to the database
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            #database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        # Create a cursor
        cursor = connection.cursor()

        # Execute the SQL statement to create the user registration table
        cursor.execute(create_user_table_sql)

        # Commit the changes
        connection.commit()

        print("Tables created successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error creating tables:", error)

    finally:
        # Close cursor/conn after use
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    create_tables()
