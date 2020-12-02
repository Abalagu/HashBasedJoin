# Created by Luming on 11/17/2020 7:42 PM
from typing import List, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor


def pg_connect():
    conn: connection = psycopg2.connect(database='hash_based_join',
                                        user='postgres',
                                        password='xuluming',
                                        host='localhost',
                                        port='5432')
    return conn


def init_pg_tables():
    conn = pg_connect()
    cur = conn.cursor()
    with open('script.sql', 'r') as f:
        cur.execute(f.read())
        conn.commit()

    print("TABLES INIT.")


def relation_to_pg(table_name: str, tuples: List[Tuple]):
    conn = pg_connect()
    cur = conn.cursor()
    sql = "insert into {} values (%s,%s)".format(table_name)
    for t in tuples:
        # ret = cur.mogrify(sql, ('A1234', 1,))
        cur.execute(sql, t)
    else:
        conn.commit()


if __name__ == '__main__':
    init_pg_tables()
