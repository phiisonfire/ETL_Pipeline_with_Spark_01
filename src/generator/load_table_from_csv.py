from src.utils import database_connector
import yaml

import sys
from src.exception import CustomException
from src.logger import logging

def load_table_from_csv():
    try:
        conn = database_connector.connect_to_database()
        print("Connected to the database")
        cursor = conn.cursor()
        print(type(cursor))
    except Exception as e:
        
        raise CustomException(e, sys)

if __name__ == "__main__":
    load_table_from_csv()