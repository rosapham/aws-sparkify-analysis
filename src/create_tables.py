import configparser
from typing import Any

import psycopg2

from sql_queries import create_table_queries, drop_table_queries

"""
Drop all tables if existed in the database
Params: cur: cursor object, conn: connection object
"""


def drop_tables(cur: Any, conn: Any) -> None:
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


"""
Create staging and analytics tables in the database
Params: cur: cursor object, conn: connection object
"""


def create_tables(cur: Any, conn: Any) -> None:
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


"""
Connect to the database in Redshift
Drop all tables if existed and create new tables
"""


def main() -> None:
    config = configparser.ConfigParser()
    config.read("./dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config["CLUSTER"].values())
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
