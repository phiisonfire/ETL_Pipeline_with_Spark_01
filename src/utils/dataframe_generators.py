import pandas as pd
from faker import Faker
from random import randint
import csv

"""
Customer:
  CustomerID: "integer PRIMARY KEY AUTO_INCREMENT UNIQUE"
  Name: "VARCHAR(255)"
  Email: "VARCHAR(255)"
  Age: "integer"
  ModifiedDate: "datetime"

SalesOrderHeader:
  SalesOrderID: "integer PRIMARY KEY AUTO_INCREMENT UNIQUE"
  OrderDate: "datetime"
  CustomerID: "integer FOREIGN KEY REFERENCES Customer(CustomerID)"

SalesOrderDetail:
  SalesOrderID: "integer FOREIGN KEY REFERENCES SalesOrderHeader(SalesOrderID)"
  SalesOrderLineNumber: "integer"
  ProductKey: "integer FOREIGN KEY REFERENCES Product(ProductKey)"
  Qty: "integer"
  ModifiedDate: "datetime"
"""

Customer_columns = {
    'CustomerID': 'int64',
    'Name': 'object',
    'Email': 'object',
    'Age': 'int64',
    'ModifiedDate': 'object'    
}

SalesOrderHeader_columns = {
    'SalesOrderID': 'int64',
    'OrderDate': 'object',
    'CustomerID': 'int64'
}

SalesOrderDetail_columns = {
    'SalesOrderID': 'int64',
    'SalesOrderLineNumber': 'int64',
    'ProductKey': 'int64',
    'Qty': 'int64',
    'ModifiedDate': 'object'
}

def get_Customer_df():
    return pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in Customer_columns.items()})

def get_SalesOrderHeader_df():
    return pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in SalesOrderHeader_columns.items()})

def get_SalesOrderDetail_df():
    return pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in SalesOrderDetail_columns.items()})

def generate_Customer_row(customer_id: int, modified_date: str):
    fake = Faker()
    return {
    'CustomerID': customer_id,
    'Name': fake.name(),
    'Email': fake.email(),
    'Age': randint(17, 90),
    'ModifiedDate': modified_date
}
    
def generate_SalesOrderHeader_row(sale_order_id: int, order_date: str, customer_id: int):
    return {
    'SalesOrderID': sale_order_id,
    'OrderDate': order_date,
    'CustomerID': customer_id
}
    
def generate_SalesOrderDetail_row(sale_order_id: int, sales_order_line_number: int, product_key: int, qty: int, modified_date: str):
    return {
    'SalesOrderID': sale_order_id,
    'SalesOrderLineNumber': sales_order_line_number,
    'ProductKey': product_key,
    'Qty': qty,
    'ModifiedDate': modified_date
}
    
def save_dataframe_as_csv(save_path: str, filename: str, df: pd.DataFrame, modified_date: str):
    full_file_name = save_path + filename + "_" + modified_date + ".csv"
    df.to_csv(full_file_name, index=False, sep=',', quotechar='"', lineterminator='\n', quoting=csv.QUOTE_NONNUMERIC)