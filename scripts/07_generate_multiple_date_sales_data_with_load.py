import argparse
from src.utils.db_connection import DBConnection
from src.generator.generator import DataGenerator
import time
from datetime import datetime, timedelta

def main(n_each_day: int, start_date: str, end_date: str, n_processes: int = 8):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    number_of_days = (end_date - start_date).days + 1  # Include end_date in the range
    
    # Generate list of dates
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(number_of_days)]
    for order_date in date_list:
        conn = DBConnection()
        generator = DataGenerator(conn)
        generator.generate_and_load(n_orders=n_each_day, order_date=order_date, n_processes=n_processes)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sales data.')
    parser.add_argument('--n_each_day', type=int, required=True, help='Number of sales orders to be created.')
    parser.add_argument('--start_date', type=str, required=True, help='Date to generate data, format as YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, required=True, help='Date to generate data, format as YYYY-MM-DD')
    parser.add_argument('--n_processes', type=int, required=False, default=8, help='Number of processes to generate data')
    
    args = parser.parse_args()
    
    start_time = time.time()
    main(args.n_each_day, args.start_date, args.end_date, args.n_processes)
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")