import mysql.connector
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_database():
    try:
        load_dotenv()
        conn = mysql.connector.connect(
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            host=os.getenv('MYSQL_HOST'),
            database=os.getenv('MYSQL_DATABASE')
        )
        logger.info("Successfully connected to the database.")
        return conn
    except mysql.connector.Error as err:
        logger.error("Error connecting to the database: %s", err)
        return None

if __name__ == "__main__":
    # Test the database connection
    connection = connect_to_database()
    if connection:
        print("Successfully connected to the database.")
        connection.close()
    else:
        print("Failed to connect to the database.")