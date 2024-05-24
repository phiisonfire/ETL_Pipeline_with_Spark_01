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
                database=self.database,
                allow_local_infile=True  # Enable local infile
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
    
    def execute_query(self, query, on_schema=None, params=None, is_load=False):
        cursor = self.connection.cursor(buffered=True)
        try:
            if on_schema:
                cursor.execute(f"USE {on_schema}")
                print(f"USE {on_schema}")
            if is_load:
                cursor.execute("SET foreign_key_checks = 0")
                cursor.execute("SET unique_checks = 0")
                cursor.execute("SET autocommit = 0")
                cursor.execute("SET sql_log_bin = 0")
            cursor.execute(query, params)
            if is_load:
                cursor.execute("SET foreign_key_checks = 1")
                cursor.execute("SET unique_checks = 1")
                cursor.execute("COMMIT")
                cursor.execute("SET autocommit = 1")
                cursor.execute("SET sql_log_bin = 1")
            self.connection.commit()
            return cursor.fetchall() if query.strip().lower().startswith("select") else None
        except Error as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()
            raise Exception(f"Error executing query: {e}")
        finally:
            cursor.close()
    
if __name__ == "__main__":
    conn = DBConnection()
    try:
        conn.connect()
        conn.list_schemas()
    finally:
        conn.close()