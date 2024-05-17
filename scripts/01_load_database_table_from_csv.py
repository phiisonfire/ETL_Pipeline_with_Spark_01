import argparse
from src.utils.db_connection import DBConnection
from src.generator.generator import DataGenerator

def main(csv_table_file_path, csv_table_schema_file_path, table_name):
    conn = DBConnection()
    generator = DataGenerator(conn)
    generator.load_csv_to_db(
        csv_table_file_path=csv_table_file_path,
        csv_table_schema_file_path=csv_table_schema_file_path,
        table_name=table_name
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load CSV data to database.')
    parser.add_argument('--csv_table_file_path', type=str, required=True, help='Path to the CSV file.')
    parser.add_argument('--csv_table_schema_file_path', type=str, required=True, help='Path to the CSV table schema file.')
    parser.add_argument('--table_name', type=str, required=True, help='Name of the table in the database.')
    
    args = parser.parse_args()
    
    main(args.csv_table_file_path, args.csv_table_schema_file_path, args.table_name)
