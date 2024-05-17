from src.utils.database_connector import connect_to_database
import yaml

import sys
from src.exception import CustomException
from src.logger import logging
import argparse

def load_table_from_csv(csv_file_path: str, yaml_file_path: str, table_name: str) -> None:
    try:
        conn = connect_to_database()
        if conn is None:
            raise Exception("Cannot connect to database")
        print("Connected to the database.")
        logging.info("Connected to the database.")
        cursor = conn.cursor()
        
        # Create table if not exists
        with open(yaml_file_path, 'r') as yaml_file:
            yaml_data = yaml.safe_load(yaml_file)
            
            valid_columns = [col for col in yaml_data['columns'] if isinstance(col, dict)]
            
            columns = ', '.join([f"{col['name']} {col['type']}" for col in valid_columns])
            
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            cursor.execute(create_table_query)
        
        # Load data from CSV
        load_data_query = f"""
            LOAD DATA INFILE '{csv_file_path}'
            INTO TABLE {table_name}
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            """
        cursor.execute(load_data_query)
        conn.commit()
        print(f"Data loaded successfully from CSV to table '{table_name}'.")
            
    except Exception as e:
        raise e
    finally:
        if conn:
            conn.close()

csv_file_path = "/home/phinguyen/ETL_Pipeline_with_Spark_01/sample_data/DimProducts.csv"
yaml_file_path = "/home/phinguyen/ETL_Pipeline_with_Spark_01/sample_data/DimProducts.yaml"
table_name = "DimProducts"
load_table_from_csv(csv_file_path, yaml_file_path, table_name)