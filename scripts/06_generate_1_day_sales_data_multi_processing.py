import argparse
from src.utils.db_connection import DBConnection
from src.generator.generator import DataGenerator
import time
import multiprocessing

def worker(n, order_date, batch_size):
    conn = DBConnection()
    generator = DataGenerator(conn)
    generator.generate_sales_data(n=n, order_date=order_date, batch_size=batch_size)

def main(n: int, order_date: str, batch_size=10000, num_processes=4):
    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=worker, args=(n, order_date, batch_size))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sales data with multiprocessing.')
    parser.add_argument('--n_orders', type=int, required=True, help='Number of sales orders to be created.')
    parser.add_argument('--order_date', type=str, required=True, help='Date to generate data, format as YYYY-MM-DD')
    parser.add_argument('--batch_size', type=int, required=False, default=10000, help='Batch number of SQL Query to be executed in a connection.')
    parser.add_argument('--num_processes', type=int, default=4, help='Number of processes to use for data generation.')

    args = parser.parse_args()

    start_time = time.time()
    main(args.n_orders, args.order_date, args.batch_size, args.num_processes)
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

