import argparse
from src.utils.db_connection import DBConnection
from src.generator.generator import DataGenerator

def main(n: int, order_date: str):
    conn = DBConnection()
    generator = DataGenerator(conn)
    generator.generate_sales_data(n=n, order_date=order_date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sales data.')
    parser.add_argument('--n_orders', type=int, required=True, help='Path to the CSV file.')
    parser.add_argument('--order_date', type=str, required=True, help='Format as YYYY-MM-DD')
    
    args = parser.parse_args()
    
    main(args.n_orders, args.order_date)

