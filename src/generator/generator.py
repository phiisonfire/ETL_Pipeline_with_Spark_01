from src.logger import logging
from mysql.connector import Error
from src.utils.db_connection import DBConnection
from src.utils.db_query_generators import generate_customer_info, generate_sales_order_header, generate_sales_order_lines
from src.generator.schema import load_table_schema_from_yaml, generate_create_table_sql
import pandas as pd
import numpy as np
import random
from tqdm import tqdm

from datetime import datetime

class DataGenerator:
    def __init__(self, conn: DBConnection) -> None:
        self.conn = conn
    
    def load_csv_to_db(self, csv_table_file_path: str, csv_table_schema_file_path: str, table_name: str):
        table_schema = load_table_schema_from_yaml(csv_table_schema_file_path)
        # table_creation_query = generate_create_table_sql(table_name=table_name, schema=table_schema)
        
        # Connect to MySQL Database
        self.conn.connect()
        
        try:
            # self.conn.execute_query(table_creation_query)
            
            # Load CSV
            data = pd.read_csv(csv_table_file_path, na_values=['NA', 'NULL'])
            data.replace({np.nan: None}, inplace=True)
            columns = [column['name'] for column in table_schema['columns']]
            data = data[columns]  # Ensure the CSV data matches the schema
            
            # Iterate through each row in the DataFrame
            record_count = 0
            for index, row in data.iterrows():
                record_count += 1
                # Construct the SQL query
                values = ', '.join(['NULL' if v is None else "\""+str(v).replace("'", "''")+"\"" for v in row])
                columns_str = ', '.join(columns)
                sql_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values})"
                self.conn.execute_query(sql_query)
            logging.info(f"Write {record_count} rows into table {table_name} in database {self.conn.database}")
            
        finally:
            self.conn.close()
    
    def generate_sales_data(self, n: int, order_date: str):
        # convert order_date from str to datetime
        order_date = datetime.strptime(order_date, "%Y-%m-%d")
        
        # Connect to MySQL Database
        self.conn.connect()
        
        try:
            # get the latest CustomerID and SalesOrderID
            max_customer_id = self.conn.execute_query("SELECT MAX(CustomerID) FROM Customer")[0][0]
            max_sales_order_id = self.conn.execute_query("SELECT MAX(SalesOrderID) FROM SalesOrderHeader")[0][0]
            
            print(f"Start generating {n} sales orders for {order_date}")
            for i in tqdm(range(n)):
                # 40% chance that the order is created by a new customer
                is_new_customer = random.randint(1,10) > 6
                
                if is_new_customer:
                    # get an id for the new customer
                    max_customer_id += 1
                    curr_customer_id = max_customer_id
                    # insert information of the new customer into table Customer
                    customer_insert_query = generate_customer_info(customer_id=curr_customer_id, 
                                                                modified_date=order_date)
                    self.conn.execute_query(customer_insert_query)
                else:
                    # pick random a customer from current customers
                    curr_customer_id = random.randint(1, max_customer_id)
                
                # create 1 sales order for this current customer
                max_sales_order_id += 1
                sales_order_header_insert_query = generate_sales_order_header(sales_order_id=max_sales_order_id, 
                                                                            modified_date=order_date, 
                                                                            customer_id=curr_customer_id)
                self.conn.execute_query(sales_order_header_insert_query)
                
                # create sales order lines for this sales order
                sales_order_line_insert_queries = generate_sales_order_lines(sales_order_id=max_sales_order_id,
                                                                        modified_date=order_date)
                for line in (sales_order_line_insert_queries):
                    self.conn.execute_query(line)
            print("Finish generating sales data.")
            logging.info(f"Generated {n} sales orders for {order_date}")
        except Exception as e:
            print(f"Error generating sales data: {e}")
            logging.exception(f"Exception generating sales data {e}")
            raise Exception(f"Error executing query: {e}")
            
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