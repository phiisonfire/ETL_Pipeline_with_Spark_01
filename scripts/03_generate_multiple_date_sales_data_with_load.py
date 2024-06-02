import argparse
from src.utils.db_connection import DBConnection
from src.generator.generator import DataGenerator
import time

def main(n_each_day: int, start_date: str, end_date: str):
    conn = DBConnection()
    generator = DataGenerator(conn)
    generator.generate_and_load(n_each_day, start_date, end_date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sales data.')
    parser.add_argument('--n_each_day', '-n', type=int, required=True, help='Number of sales orders to be created.')
    parser.add_argument('--start_date', '-sdate', type=str, required=True, help='Date to generate data, format as YYYY-MM-DD')
    parser.add_argument('--end_date', '-edate', type=str, required=True, help='Date to generate data, format as YYYY-MM-DD')
    
    args = parser.parse_args()
    
    start_time = time.time()
    main(args.n_each_day, args.start_date, args.end_date)
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")