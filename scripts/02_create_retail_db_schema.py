import argparse
import yaml
from src.utils.db_connection import DBConnection

def create_table(cursor, table_name, fields):
    query = f"CREATE TABLE {table_name} ("
    columns = []
    foreign_keys = []
    for name, type in fields.items():
        if 'FOREIGN KEY' in type:
            foreign_key, references = type.split('REFERENCES')
            foreign_keys.append(f"FOREIGN KEY ({name}) REFERENCES {references}")
            columns.append(f"{name} integer")
        else:
            columns.append(f"{name} {type}")
    query += ", ".join(columns + foreign_keys)
    query += ")"
    print(query)
    cursor.execute(query)

def create_tables(schema_file):
    conn = DBConnection()
    conn.connect()

    cursor = conn.connection.cursor()

    with open(schema_file, 'r') as file:
        schema = yaml.safe_load(file)

    for table_name, fields in schema.items():
        create_table(cursor, table_name, fields)
        print(f"Created table {table_name} in database {conn.database}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create retail_db schema.')
    parser.add_argument('--database_schema_path', type=str, required=True, help='Path to database schema')
    args = parser.parse_args()
    create_tables(args.database_schema_path)