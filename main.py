import sys

from psycopg2 import Error

from db import get_connect_to_db, get_parsed_row_by_id, get_employee_names_by_city, get_row_with_city_by_id_employee
from exceptions import RowNotFound
from config import DB, PORT_DB, PASSWORD_DB, HOST_DB, USER_DB

try:
    connection_db = get_connect_to_db(user=USER_DB,
                                      password=PASSWORD_DB,
                                      host=HOST_DB,
                                      port=PORT_DB,
                                      database=DB)
    id_employee = int(sys.argv[1])
    cursor_db = connection_db.cursor()
    parsed_row = get_parsed_row_by_id(id_employee, cursor_db)
    id_employee = parsed_row.id_row
    parent = parsed_row.parent
    name = parsed_row.name
    type_row = parsed_row.type_row
    if type_row != 3:
        print(f"{id_employee} не является сотрудником")
        exit()
    row_city = get_row_with_city_by_id_employee(parent, cursor_db)
    employees = get_employee_names_by_city(row_city.id_row, cursor_db)
    response = f"{row_city.name}: {employees}."
    print(response)
except Error as e:
    print("Ошибка при подключении", e)
    exit()
except IndexError as e:
    print("Введите ID работника")
    exit()
except ValueError as e:
    print("Введите числовое значение")
    exit()
except RowNotFound as e:
    print(f"Не найдено записи с ID={id_employee}")
    exit()





