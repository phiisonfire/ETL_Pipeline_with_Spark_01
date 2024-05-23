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
    
    def generate_sales_data_process(
        self,
        n_orders_per_process: int,
        order_date: str,
        max_customer_id_shared,
        start_sales_order_id: int,
        lock,
        process_id: int,
        result_queue
    ) -> None:      
        fake = Faker()
        customer_data = []
        sales_order_header_data = []
        sales_order_detail_data = []

        for i in range(n_orders_per_process):
            if randint(1, 10) > 8:
                lock.acquire()
                max_customer_id_shared.value += 1
                curr_customer_id = max_customer_id_shared.value
                lock.release()
                new_customer = (
                    curr_customer_id,
                    fake.name(),
                    fake.email(),
                    randint(18, 90),
                    order_date
                )
                customer_data.append(new_customer)
            else:
                curr_customer_id = randint(1, max_customer_id_shared.value)

            new_sales_order_header = (
                start_sales_order_id + i,
                order_date,
                curr_customer_id
            )
            sales_order_header_data.append(new_sales_order_header)

            n_lines = randint(1, 10)
            for line_number in range(n_lines):
                new_sales_order_detail = (
                    start_sales_order_id + i,
                    line_number + 1,
                    randint(1, 606),
                    randint(1, 20),
                    order_date
                )
                sales_order_detail_data.append(new_sales_order_detail)

        result_queue.put((process_id, customer_data, sales_order_header_data, sales_order_detail_data))
        
    def save_to_csv(self, order_date: str, result_queue, n_processes: int) -> List[str]:
        print("Start writing data to .csv files")
        customers_data = []
        all_sales_order_header_data = []
        all_sales_order_detail_data = []

        for _ in range(n_processes):
            process_id, customer_data, sales_order_header_data, sales_order_detail_data = result_queue.get()
            customers_data.extend(customer_data)
            all_sales_order_header_data.extend(sales_order_header_data)
            all_sales_order_detail_data.extend(sales_order_detail_data)

        customers_df = pd.DataFrame(customers_data, columns=["CustomerID", "Name", "Email", "Age", "ModifiedDate"])
        customers_df.sort_values(by='CustomerID', ascending=True, inplace=True)
        sales_order_header_df = pd.DataFrame(all_sales_order_header_data, columns=["SalesOrderID", "OrderDate", "CustomerID"])
        sales_order_header_df.sort_values(by='SalesOrderID', ascending=True, inplace=True)
        sales_order_detail_df = pd.DataFrame(all_sales_order_detail_data, columns=["SalesOrderID", "SalesOrderLineNumber", "ProductKey", "Qty", "ModifiedDate"])
        sales_order_detail_df.sort_values(by=['SalesOrderID', 'SalesOrderLineNumber'], ascending=[True, True], inplace=True)

        customder_csv_file_path = "/tmp/Customer_" + order_date + ".csv"
        salesorderheader_csv_file_path = "/tmp/SalesOrderHeader_" + order_date + ".csv"
        salesorderdetail_csv_file_path = "/tmp/SalesOrderDetail_" + order_date + ".csv"
        
        customers_df.to_csv(customder_csv_file_path, index=False, sep=',', quotechar='"', lineterminator='\n', quoting=csv.QUOTE_ALL)
        sales_order_header_df.to_csv(salesorderheader_csv_file_path, index=False, sep=',', quotechar='"', lineterminator='\n', quoting=csv.QUOTE_ALL)
        sales_order_detail_df.to_csv(salesorderdetail_csv_file_path, index=False, sep=',', quotechar='"', lineterminator='\n', quoting=csv.QUOTE_ALL)
        print("Finish writing data to .csv files")
        return [customder_csv_file_path, salesorderheader_csv_file_path, salesorderdetail_csv_file_path]
    
    def generate_1_day_sales_data(
        self,
        n_orders: int, 
        order_date: str,
        n_processes: int
    ) -> Dict:
        
        # Get current max_customer_id and max_sales_order_id from mysql database
        try:
            max_customer_id = self.conn.execute_query("SELECT MAX(CustomerId) FROM Customer")[0][0]
            max_sales_order_id = self.conn.execute_query("SELECT MAX(SalesOrderId) FROM SalesOrderHeader")[0][0]
        except Exception as e:
            raise Exception(e)
        
        n_orders_per_process = n_orders // n_processes
        remainder_orders = n_orders % n_processes
        max_customer_id_shared = Value('i', max_customer_id)
        manager = Manager()
        result_queue = manager.Queue()
        processes = []
        lock = Lock()
        
        for i in range(n_processes):
            additional_order = 1 if i < remainder_orders else 0
            start_sales_order_id = max_sales_order_id + i * (n_orders_per_process + additional_order)
            args = (n_orders_per_process + additional_order, order_date, max_customer_id_shared, start_sales_order_id, lock, i, result_queue)
            p = Process(target=self.generate_sales_data_process, args=args)
            p.start()
            processes.append(p)
        
        for p in processes:
            p.join()
        
        file_paths = self.save_to_csv(order_date=order_date, result_queue=result_queue, n_processes=n_processes)
        table_names = ["Customer", "SalesOrderHeader", "SalesOrderDetail"]
        return dict(zip(table_names, file_paths))
    
    def generate_and_load(
        self,
        n_orders: int, 
        start_date: str,
        end_date: str,
        n_processes: int
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
                table_names_and_paths = self.generate_1_day_sales_data(n_orders, order_date, n_processes)
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