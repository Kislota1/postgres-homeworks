import psycopg2
import csv


class Database:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = psycopg2.connect(
            host="localhost",
            database="north",
            user="postgres",
            password="9517530qscfT"
        )
        self.cursor = self.connection.cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def commit(self):
        if self.connection:
            self.connection.commit()

    def rollback(self):
        if self.connection:
            self.connection.rollback()


def insert_employees(db, employees_data):
    with open(employees_data, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            db.cursor.execute(
                "INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s, %s)",
                row
            )


def insert_customers(db, customers_data):
    with open(customers_data, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            db.cursor.execute(
                "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
                row
            )


def insert_orders(db, orders_data):
    with open(orders_data, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            db.cursor.execute(
                "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s, %s, %s)",
                row
            )


def main():
    db = Database()
    db.connect()

    try:
        insert_employees(db, 'north_data/employees_data.csv')
        insert_customers(db, 'north_data/customers_data.csv')
        insert_orders(db, 'north_data/orders_data.csv')

        db.commit()
    finally:
        db.close()

