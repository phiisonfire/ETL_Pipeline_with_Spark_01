from src.logger import logging
from src.utils.db_connection import DBConnection
from src.generator.schema import load_table_schema_from_yaml, generate_create_table_sql
import pandas as pd
import numpy as np

class DataGenerator:
    def __init__(self, conn: DBConnection) -> None:
        self.conn = conn
    
    def load_csv_to_db(self, csv_table_file_path: str, csv_table_schema_file_path: str, table_name: str):
        table_schema = load_table_schema_from_yaml(csv_table_schema_file_path)
        table_creation_query = generate_create_table_sql(table_name=table_name, schema=table_schema)
        
        # Connect to MySQL Database
        self.conn.connect()
        
        try:
            self.conn.execute_query(table_creation_query)
            
            # Load CSV
            data = pd.read_csv(csv_table_file_path, na_values=['NA', 'NULL'])
            data.replace({np.nan: None}, inplace=True)
            columns = [column['name'] for column in table_schema['columns']]
            data = data[columns]  # Ensure the CSV data matches the schema
            
            # Iterate through each row in the DataFrame
            for index, row in data.iterrows():
                # Construct the SQL query
                values = ', '.join(['NULL' if v is None else "\""+str(v).replace("'", "''")+"\"" for v in row])
                columns_str = ', '.join(columns)
                sql_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values})"
                self.conn.execute_query(sql_query)
            
        finally:
            self.conn.close()

if __name__ == "__main__":
    conn = DBConnection()
    generator = DataGenerator(conn)
    generator.load_csv_to_db(
        csv_table_file_path="/home/phinguyen/ETL_Pipeline_with_Spark_01/sample_data/DimProducts.csv",
        csv_table_schema_file_path="/home/phinguyen/ETL_Pipeline_with_Spark_01/sample_data/DimProducts.yaml",
        table_name="DimProducts"
    )