import mysql.connector
from mysql.connector.connection_cext import CMySQLConnection
from dotenv import load_dotenv
import os
from src.logger import logging
from src.exception import CustomException
import sys

def connect_to_database() -> CMySQLConnection:
    try:
        load_dotenv()
        conn = mysql.connector.connect(
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            host=os.getenv('MYSQL_HOST'),
            database=os.getenv('MYSQL_DATABASE'),
            local_infile=True
        )
        if conn is None:
            raise CustomException("Failed to connect to the database", sys)
        logging.info("Successfully connected to the database.")
        print(type(conn))
        return conn
    except mysql.connector.Error as err:
        logging.error("Error connecting to the database: %s", err)
        return None

if __name__ == "__main__":
    # Test the database connection
    connection = connect_to_database()
    cursor = connection.cursor()
    if connection:
        print("Successfully connected to the database.")
        connection.close()
    else:
        print("Failed to connect to the database.")