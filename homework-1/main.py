"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
from csv import DictReader

"""Пути к файлам с данными."""
emp_data_csv = "north_data/employees_data.csv"
cust_data_csv = "north_data/customers_data.csv"
ord_data_csv = "north_data/orders_data.csv"

"""Подключаемся к БД."""
conn = psycopg2.connect(host="localhost", database="north", user="postgres", password="123123")

"""Открываем на чтение файлы и загружаем данные из них в таблицы БД."""
try:
    with conn:
        with conn.cursor() as cur:
            with open(emp_data_csv, encoding="UTF-8") as file:
                values = DictReader(file)
                for value in values:
                    cur.execute(
                        "INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                        (
                            int(value["employee_id"]),
                            value["first_name"],
                            value["last_name"],
                            value["title"],
                            value["birth_date"],
                            value["notes"],
                        ),
                    )

            with open(cust_data_csv) as file:
                values = DictReader(file)
                for value in values:
                    cur.execute(
                        "INSERT INTO customers VALUES (%s, %s, %s)",
                        (value["customer_id"], value["company_name"], value["contact_name"]),
                    )

            with open(ord_data_csv) as file:
                values = DictReader(file)
                for value in values:
                    cur.execute(
                        "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                        (
                            value["order_id"],
                            value["customer_id"],
                            value["employee_id"],
                            value["order_date"],
                            value["ship_city"],
                        ),
                    )

    """Закрываем соединение с БД."""
finally:
    conn.close()
