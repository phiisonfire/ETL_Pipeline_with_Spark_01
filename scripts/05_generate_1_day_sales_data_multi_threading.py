import argparse
from src.utils.db_connection import DBConnection
from src.generator.generator import DataGenerator
import time
import threading
from queue import Queue

def worker(queue, conn):
    while True:
        n, order_date, batch_size = queue.get()
        if n is None:
            break
        generator = DataGenerator(conn)
        generator.generate_sales_data(n=n, order_date=order_date, batch_size=batch_size)
        queue.task_done()

def main(n: int, order_date: str, batch_size=10000, num_threads=4):
    queue = Queue()
    conn = DBConnection()

    for _ in range(num_threads):
        t = threading.Thread(target=worker, args=(queue, conn))
        t.daemon = True
        t.start()

    queue.put((n, order_date, batch_size))
    queue.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sales data with multithreading.')
    parser.add_argument('--n_orders', type=int, required=True, help='Number of sales orders to be created.')
    parser.add_argument('--order_date', type=str, required=True, help='Date to generate data, format as YYYY-MM-DD')
    parser.add_argument('--batch_size', type=int, required=False, default=10000, help='Batch number of SQL Query to be executed in a connection.')
    parser.add_argument('--num_threads', type=int, default=4, help='Number of threads to use for data generation.')

    args = parser.parse_args()

    start_time = time.time()
    main(args.n_orders, args.order_date, args.batch_size, args.num_threads)
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

