import configparser
from typing import Any

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries

"""
Extract data from S3 and load into staging tables
Params: cur: cursor object, conn: connection object
"""


def load_staging_tables(cur: Any, conn: Any) -> None:
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


"""
Insert data from staging tables into fact and dimension tables
Params: cur: cursor object, conn: connection object
"""


def insert_tables(cur: Any, conn: Any) -> None:
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


"""
Connect to the database in Redshift
Load data from S3 to staging tables and insert into fact and dimension tables
"""


def main() -> None:
    config = configparser.ConfigParser()
    config.read("./dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config["CLUSTER"].values())
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
