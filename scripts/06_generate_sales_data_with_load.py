import argparse
from src.utils.db_connection import DBConnection
from src.generator.generator import DataGenerator
import time

def main(n: int, order_date: str, n_processes: int = 8):
    conn = DBConnection()
    generator = DataGenerator(conn)
    generator.generate_and_load(n_orders=n, order_date=order_date, n_processes=8)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sales data.')
    parser.add_argument('--n_orders', type=int, required=True, help='Number of sales orders to be created.')
    parser.add_argument('--order_date', type=str, required=True, help='Date to generate data, format as YYYY-MM-DD')
    parser.add_argument('--n_processes', type=int, required=False, default=8, help='Batch number of SQL Query to be executed in a connection.')
    
    args = parser.parse_args()
    
    start_time = time.time()
    main(args.n_orders, args.order_date)
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")