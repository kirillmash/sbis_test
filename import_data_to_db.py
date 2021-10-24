import json

from psycopg2.extensions import connection, cursor
from psycopg2 import Error

from db import get_connect_to_db
from config import DB, PORT_DB, PASSWORD_DB, HOST_DB, USER_DB


def create_db(db_connection: connection, db_cursor: cursor) -> None:
    """Create table Employees"""
    create_table_query = '''CREATE TABLE employees
                              (
                              Id    INT           PRIMARY KEY     NOT NULL,
                              Parent INT                                  ,
                              Name   Varchar(255)                 NOT NULL,
                              Type   INT                          NOT NULL
                              ); '''
    db_cursor.execute(create_table_query)
    db_connection.commit()


def insert_data_in_table(data: str, db_connection: connection, db_cursor: cursor) -> None:
    """Insert data in table"""
    insert_query = f"""INSERT INTO employees (Id, Parent, Name, Type) VALUES
                       {data};"""
    db_cursor.execute(insert_query)
    db_connection.commit()


def parse_data_from_json() -> str:
    """parsed data from json and return str for query"""
    with open('dump_db.json', 'r', encoding='utf-8') as f:
        dump_data = json.load(f)
    parsed_data = ''
    for row in dump_data:
        if row['ParentId'] is None:
            parsed_data += f"({row['id']}, Null, '{row['Name']}',{row['Type']}),"
        else:
            parsed_data += f"{(row['id'], row['ParentId'], row['Name'], row['Type'])},"
    return parsed_data[:-1]


def drop_table(db_cursor: cursor) -> None:
    """Drop table"""
    drop_query = "DROP TABLE employees;"
    db_cursor.execute("ROLLBACK")
    db_cursor.execute(drop_query)


if __name__ == "__main__":
    try:
        connection_db = get_connect_to_db(user=USER_DB,
                                          password=PASSWORD_DB,
                                          host=HOST_DB,
                                          port=PORT_DB,
                                          database=DB)
    except Error as e:
        print("Ошибка при подключении,", e)
        exit()
    cursor_db = connection_db.cursor()
    try:
        create_db(connection_db, cursor_db)
    except Error as e:
        if '"employees" already exists' in str(e):
            drop_table(cursor_db)
            create_db(connection_db, cursor_db)
        else:
            print("Ошибка при создании таблицы,", e)
            exit()
    try:
        data_to_db = parse_data_from_json()
    except KeyError as e:
        print("Неккоректный dump базы,", e)
        exit()
    try:
        insert_data_in_table(data_to_db, connection_db, cursor_db)
    except Error as e:
        print("Ошибка загрузки данных,", e)
        exit()
    cursor_db.close()
    connection_db.close()
    print("Данные импортированы")
