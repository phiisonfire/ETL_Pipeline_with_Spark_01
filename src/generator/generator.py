from src.logger import logging
from src.utils.db_connection import DBConnection
from src.generator.schema import load_table_schema_from_yaml
import pandas as pd
import numpy as np
from random import randint
from multiprocessing import Process, Value, Lock, Manager
from faker import Faker
from typing import List, Dict
import csv
from datetime import datetime, timedelta
from tqdm import tqdm
from queue import Queue
import threading
import os

class DataGenerator:
    def __init__(self, conn: DBConnection) -> None:
        self.conn = conn
    
    def load_csv_to_db(self, csv_table_file_path: str, csv_table_schema_file_path: str, table_name: str):
        table_schema = load_table_schema_from_yaml(csv_table_schema_file_path)
        
        self.conn.connect()
        try:
            data = pd.read_csv(csv_table_file_path, na_values=['NA', 'NULL'])
            data.replace({np.nan: None}, inplace=True)
            columns = [column['name'] for column in table_schema['columns']]
            data = data[columns]  # Ensure the CSV data matches the schema
            
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
    
    def generate_data(self, n_orders: int, order_date: str, batch_size: int, queue: Queue) -> None:
        print(f"Generator Process ID: {os.getpid()}")
        try:
            max_customer_id = self.conn.execute_query("SELECT MAX(CustomerId) FROM Customer")[0][0]
            if not max_customer_id:
                max_customer_id = 0
            max_sales_order_id = self.conn.execute_query("SELECT MAX(SalesOrderId) FROM SalesOrderHeader")[0][0]
            if not max_sales_order_id:
                max_sales_order_id = 0
        except Exception as e:
            raise Exception(e)

        fake = Faker()

        for batch_start in tqdm(range(0, n_orders, batch_size), desc="Generating orders in batches"):
            customer_data = []
            sales_order_header_data = []
            sales_order_detail_data = []

            batch_end = min(batch_start + batch_size, n_orders)
            for _ in range(batch_start, batch_end):
                if randint(1, 10) > 8:
                    max_customer_id += 1
                    curr_customer_id = max_customer_id
                    new_customer = (
                        curr_customer_id,
                        fake.name(),
                        fake.email(),
                        randint(18, 90),
                        order_date
                    )
                    customer_data.append(new_customer)
                else:
                    curr_customer_id = randint(1, max_customer_id)

                max_sales_order_id += 1
                new_sales_order_header = (
                    max_sales_order_id,
                    order_date,
                    curr_customer_id
                )
                sales_order_header_data.append(new_sales_order_header)

                n_lines = randint(1, 10)
                for line_number in range(n_lines):
                    new_sales_order_detail = (
                        max_sales_order_id,
                        line_number + 1,
                        randint(1, 606),
                        randint(1, 20),
                        order_date
                    )
                    sales_order_detail_data.append(new_sales_order_detail)

            queue.put((customer_data, sales_order_header_data, sales_order_detail_data))

        queue.put(None)  # Signal that generation is done

    def write_data_to_csv(self, order_date: str, queue: Queue) -> Dict:
        print(f"Writer Process ID: {os.getpid()}")
        customer_csv_file_path = f"/tmp/Customer_{order_date}.csv"
        salesorderheader_csv_file_path = f"/tmp/SalesOrderHeader_{order_date}.csv"
        salesorderdetail_csv_file_path = f"/tmp/SalesOrderDetail_{order_date}.csv"

        with open(customer_csv_file_path, mode='w', newline='\n') as customer_file, \
             open(salesorderheader_csv_file_path, mode='w', newline='\n') as salesorderheader_file, \
             open(salesorderdetail_csv_file_path, mode='w', newline='\n') as salesorderdetail_file:
            
            customer_writer = csv.writer(customer_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n")
            salesorderheader_writer = csv.writer(salesorderheader_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n")
            salesorderdetail_writer = csv.writer(salesorderdetail_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n")

            # Write headers
            customer_writer.writerow(["CustomerID", "Name", "Email", "Age", "ModifiedDate"])
            salesorderheader_writer.writerow(["SalesOrderID", "OrderDate", "CustomerID"])
            salesorderdetail_writer.writerow(["SalesOrderID", "SalesOrderLineNumber", "ProductKey", "Qty", "ModifiedDate"])

            while True:
                data = queue.get()
                if data is None:
                    break  # Exit signal

                customer_data, sales_order_header_data, sales_order_detail_data = data

                customer_writer.writerows(customer_data)
                salesorderheader_writer.writerows(sales_order_header_data)
                salesorderdetail_writer.writerows(sales_order_detail_data)

        print("Finish writing data to .csv files")

        return {
            "customer_csv_file_path": customer_csv_file_path,
            "salesorderheader_csv_file_path": salesorderheader_csv_file_path,
            "salesorderdetail_csv_file_path": salesorderdetail_csv_file_path
        }

    def generate_1_day_sales_data(self, n_orders: int, order_date: str) -> Dict:
        print(f"Main Process ID: {os.getpid()}")
        batch_size = 100000
        queue = Queue(maxsize=10)  # Limit the queue size to control memory usage

        generator_thread = threading.Thread(target=self.generate_data, args=(n_orders, order_date, batch_size, queue), name="GeneratorThread")
        writer_thread = threading.Thread(target=self.write_data_to_csv, args=(order_date, queue), name="WriterThread")

        generator_thread.start()
        writer_thread.start()

        generator_thread.join()
        writer_thread.join()

        return {
            "Customer": f"/tmp/Customer_{order_date}.csv",
            "SalesOrderHeader": f"/tmp/SalesOrderHeader_{order_date}.csv",
            "SalesOrderDetail": f"/tmp/SalesOrderDetail_{order_date}.csv"
        }
    
    def generate_and_load(
        self,
        n_orders: int, 
        start_date: str,
        end_date: str
    ) -> None:
        try:
            self.conn.connect()
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            number_of_days = (end_date - start_date).days + 1  # Include end_date in the range
            # Generate list of dates
            date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(number_of_days)]
            
            for order_date in date_list:
                print(f"Start generating sales data for {order_date} and save into csv files.")
                table_names_and_paths = self.generate_1_day_sales_data(n_orders, order_date)
                print("Finish generating data.")
                for table, csv_path in table_names_and_paths.items():
                    load_query = f"""
                        LOAD DATA LOCAL INFILE '{csv_path}'
                        INTO TABLE {table}
                        FIELDS TERMINATED BY ','  -- specify the delimiter used in your CSV file
                        ENCLOSED BY '"'           -- specify if the fields are enclosed by a specific character
                        LINES TERMINATED BY '\n'  -- specify the line terminator
                        IGNORE 1 LINES            -- skip the header row if your CSV has a header
                        """
                    print(f"Start loading csv file from {csv_path} into table {table}")
                    self.conn.execute_query(load_query)
                    print(f"Finish loading csv file.")
        except Exception as e:
            raise Exception(e)
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