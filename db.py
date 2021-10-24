from dataclasses import dataclass

import psycopg2
from psycopg2.extensions import connection, cursor

from exceptions import RowNotFound


@dataclass
class ParsedRow:
    id_row: int
    parent: int
    name: str
    type_row: int


def get_connect_to_db(user: str, password: str, host: str, port: str, database: str) -> connection:
    """Connect to db and return connection"""
    connection_db = psycopg2.connect(user=user,
                                     password=password,
                                     host=host,
                                     port=port,
                                     database=database)
    return connection_db


def get_parsed_row_by_id(id_row: int, db_cursor: cursor) -> ParsedRow:
    """Get parsed row from db"""
    db_cursor.execute(f"SELECT id, parent, name, type FROM employees where id={id_row}")
    row = db_cursor.fetchone()
    if row is None:
        raise RowNotFound
    id_row, parent, name, type_row = row
    return ParsedRow(id_row=id_row, parent=parent, name=name, type_row=type_row)


def get_row_with_city_by_id_employee(id_parent: int, db_cursor: cursor) -> ParsedRow:
    """Get row with city from db by employee id"""
    row = get_parsed_row_by_id(id_parent, db_cursor)
    while True:
        if row.type_row == 1:
            return row
        row = get_parsed_row_by_id(row.parent, db_cursor)


def get_employee_from_db(rows: list, db_cursor: cursor) -> list:
    """Get list of row with employees"""
    employee = []
    departments = []
    for row in rows:
        if row[3] == 3:
            employee.append(row)
        else:
            departments.append(row)
    while departments:
        department = departments.pop()
        db_cursor.execute(f"SELECT * from employees where parent={department[0]}")
        rows = db_cursor.fetchall()
        employee += get_employee_from_db(rows, db_cursor)
    return employee


def get_employee_names_by_city(city_id: int, db_cursor: cursor) -> str:
    """Get str with employee names by city"""
    db_cursor.execute(f"SELECT * from employees where parent= ANY ( SELECT id FROM employees where parent={city_id});")
    rows = db_cursor.fetchall()
    employees = get_employee_from_db(rows, db_cursor)
    names = [employee[2] for employee in employees]
    return ", ".join(names)
