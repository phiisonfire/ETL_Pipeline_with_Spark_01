import argparse
from src.utils.db_connection import DBConnection
from src.generator.generator import DataGenerator
import time
from datetime import datetime, timedelta

def main(n: int, start_date: str, end_date: str, batch_size=10000, n_processes=8):
    # Parse the start and end dates
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate a list of dates between start and end dates
    date_list = []
    current_dt = start_dt
    while current_dt <= end_dt:
        date_list.append(current_dt.strftime("%Y-%m-%d"))
        current_dt += timedelta(days=1)

    for date in date_list:
        conn = DBConnection()
        generator = DataGenerator(conn)
        generator.generate_sales_data_multiprocessing(n=n, order_date=date, batch_size=batch_size, n_processes=n_processes)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sales data.')
    parser.add_argument('--n_orders', type=int, required=True, help='Number of sales orders to be created.')
    parser.add_argument('--start_date', type=str, required=True, help='Date to generate data, format as YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, required=True, help='Date to generate data, format as YYYY-MM-DD')
    parser.add_argument('--batch_size', type=int, required=False, help='Batch number of SQL Query to be executed in a connection.')
    parser.add_argument('--n_processes', type=int, required=True, help='Number of processes.')
    args = parser.parse_args()
    
    start_time = time.time()
    main(n=args.n_orders, start_date=args.start_date, end_date=args.end_date, n_processes=args.n_processes)
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")