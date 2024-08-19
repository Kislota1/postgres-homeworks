import psycopg2
import json
from psycopg2 import sql
from config import config

def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def create_database(params, db_name):
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', user=params['user'], password=params['password'])
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute(sql.SQL("CREATE DATABASE {db_name}").format(db_name=sql.Identifier(db_name)))
    except Exception as error:
        print(f"Ошибка при создании базы данных {db_name}: {error}")
    finally:
        cur.close()
        conn.close()

def execute_sql_script(cur, script_file):
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r') as file:
        cur.execute(file.read())

def create_suppliers_table(cur):
    """Создает таблицу suppliers."""
    cur.execute("""
        CREATE TABLE suppliers (
            supplier_id SERIAL PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            contact VARCHAR(255),
            address VARCHAR(255),
            phone VARCHAR(50),
            fax VARCHAR(50),
            homepage VARCHAR(255)
        );
    """)

def get_suppliers_data(json_file: str):
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r') as file:
        return json.load(file)

def insert_suppliers_data(cur, suppliers: list):
    """Добавляет данные из suppliers в таблицу suppliers."""
    for supplier in suppliers:
        cur.execute("""
            INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            supplier['company_name'],
            supplier['contact'],
            supplier['address'],
            supplier['phone'],
            supplier['fax'],
            supplier['homepage']
        ))

def add_foreign_keys(cur):
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute("""
        ALTER TABLE products
        ADD COLUMN supplier_id INTEGER,
        ADD CONSTRAINT fk_supplier
        FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id);
    """)

if __name__ == '__main__':
    main()
