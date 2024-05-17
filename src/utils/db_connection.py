import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
from src.logger import logging

load_dotenv()

class DBConnection:
    def __init__(self) -> None:
        self.user = os.getenv('MYSQL_USER')
        self.password = os.getenv('MYSQL_PASSWORD')
        self.host = os.getenv('MYSQL_HOST')
        self.database = os.getenv('MYSQL_DATABASE')
        self.connection = None
    
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database
            )
            if self.connection.is_connected():
                print("Connection to MySQL database is successful")
                logging.info("Connection to MySQL database is successful")
                
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            logging.info(f"Error while connecting to MySQL: {e}")
            raise Exception(f"Error while connecting to MySQL: {e}")
    
    def list_schemas(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW DATABASES")
            schemas = cursor.fetchall()
            for schema in schemas:
                print(schema[0])
        except Error as e:
            print(f"Error fetching schemas: {e}")
            logging.info(f"Error fetching schemas: {e}")
            raise Exception(f"Error while fetching schemas: {e}")
        finally:
            cursor.close()
    
    def close(self):
        if self.connection.is_connected():
            self.connection.close()
            print("MySQL connection is closed")
            logging.info("MySQL connection is closed")
    
    def execute_query(self, query, params=None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        except Error as e:
            print(f"Error executing query: {e}")
        finally:
            cursor.close()
    
    

if __name__ == "__main__":
    conn = DBConnection()
    try:
        conn.connect()
        conn.list_schemas()
    finally:
        conn.close()