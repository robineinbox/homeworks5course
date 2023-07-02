"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
from pathlib import Path
import psycopg2


# Подключение к базе данных.
conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="KimNikol2"
)
cur = conn.cursor()

# Формирование абсолютного пути к файлам
data_folder = Path("north_data")
employees_data_path = data_folder / "employees_data.csv"
customers_data_path = data_folder / "customers_data.csv"
orders_data_path = data_folder / "orders_data.csv"

# Заполнение таблиц данными
with open(employees_data_path, 'r') as file:
    reader = csv.reader(file)
    next(reader)
    for line in reader:
        cur.execute(
            "INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s, %s)",
            (line[0], line[1], line[2], line[3], line[4], line[5])
        )

with open(customers_data_path, 'r') as file:
    reader = csv.reader(file)
    next(reader)
    for line in reader:
        cur.execute(
            "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
            (line[0], line[1], line[2])
        )

with open(orders_data_path, 'r') as file:
    reader = csv.reader(file)
    next(reader)
    for line in reader:
        cur.execute(
            "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s, TO_DATE(%s, 'YYYY-MM-DD'), %s)",
            (line[0], line[1], line[2], line[3], line[4])
        )

conn.commit()

# Проверка данных в таблицах
cur.execute("SELECT * FROM employees")
print(cur.fetchall())

cur.execute("SELECT * FROM customers")
print(cur.fetchall())

cur.execute("SELECT * FROM orders")
print(cur.fetchall())

cur.close()
conn.close()