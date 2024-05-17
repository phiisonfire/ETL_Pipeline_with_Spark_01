import yaml

def load_table_schema_from_yaml(file_path: str):
    """
    Load schema from a YAML file.

    Args:
    - file_path (str): Path to the YAML file containing the schema.

    Returns:
    - dict: Loaded schema.
    """
    with open(file_path, 'r') as file:
        schema = yaml.safe_load(file)
    return schema

def generate_create_table_sql(table_name, schema):
    """
    Generate SQL statement to create a table based on the schema.

    Args:
    - table_name (str): Name of the table.
    - schema (dict): Schema of the table, containing 'columns' list.

    Returns:
    - str: SQL statement to create the table.
    """
    columns = schema.get('columns', [])
    column_definitions = []
    for column in columns:
        column_name = column['name']
        column_type = column['type']
        column_definitions.append(f"{column_name} {column_type}")
    column_definitions_str = ", ".join(column_definitions)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions_str});"
    return create_table_sql

if __name__ == "__main__":
    schema = load_table_schema_from_yaml("/home/phinguyen/ETL_Pipeline_with_Spark_01/sample_data/DimProducts.yaml")
    sql_query = generate_create_table_sql(table_name="DimProducts", schema=schema)
    print(sql_query)
    