import random
from datetime import datetime
from faker import Faker

def generate_customer_info(customer_id: int, modified_date: datetime):
    fake = Faker()
    name = fake.name()
    email = fake.email()
    age = random.randint(17, 90)
    return f"({customer_id}, \"{name}\", \"{email}\", {age}, '{modified_date.strftime('%Y-%m-%d %H:%M:%S')}')"

def generate_sales_order_header(sales_order_id: int, modified_date: datetime, customer_id: int):
    return f"({sales_order_id}, '{modified_date.strftime('%Y-%m-%d %H:%M:%S')}', {customer_id})"

def generate_sales_order_lines(sales_order_id: int, modified_date: datetime):
    lines_count = random.randint(1, 10)
    products = []
    lines = []
    for line in range(1, lines_count + 1):
        product = random.randint(1, 606)
        while product in products:
            product = random.randint(1, 606)
        products.append(product)
        
        qty = random.randint(1, 20)
        lines.append(f"({sales_order_id}, {line}, {product}, {qty}, '{modified_date.strftime('%Y-%m-%d %H:%M:%S')}')")
    return lines