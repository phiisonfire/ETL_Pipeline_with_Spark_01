import random
from datetime import datetime
from faker import Faker

def generate_customer_info(customer_id: int, modified_date: datetime):
    fake = Faker()
    name = fake.name()
    email = fake.email()
    age = random.randint(17, 90)
    insert_query = (
            f"INSERT INTO Customer (CustomerID, Name, Email, Age, ModifiedDate) "
            f"VALUES ({customer_id}, \"{name}\", \"{email}\", {age}, '{modified_date.strftime('%Y-%m-%d %H:%M:%S')}')"
        )
    return insert_query

def generate_sales_order_header(sales_order_id: int, modified_date: datetime, customer_id: int):
    insert_query = (
            f"INSERT INTO SalesOrderHeader (SalesOrderID, OrderDate, CustomerID) "
            f"VALUES ({sales_order_id}, '{modified_date.strftime('%Y-%m-%d %H:%M:%S')}', {customer_id})"
        )
    return insert_query

def generate_sales_order_lines(sales_order_id: int, modified_date: datetime):
    lines_count = random.randint(1, 10)
    products = []
    line_insert_queries = []
    for line in range(1, lines_count + 1):
        product = random.randint(1, 606)
        while product in products:
            product = random.randint(1, 606)
        products.append(product)
        
        qty = random.randint(1, 20)
        insert_query = (
            f"INSERT INTO SalesOrderDetail (SalesOrderID, SalesOrderLineNumber, ProductKey, Qty, ModifiedDate) "
            f"VALUES ({sales_order_id}, {line}, {product}, {qty}, '{modified_date.strftime('%Y-%m-%d %H:%M:%S')}');"
        )
        line_insert_queries.append(insert_query)
    return line_insert_queries