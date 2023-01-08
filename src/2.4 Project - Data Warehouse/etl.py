import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load the staging tables for log data and songs from input files
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Finished loading {}".format(query))


def insert_tables(cur, conn):
    """
    Fill star schema tables using data from staging tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Finished loading {}".format(query))


def main():
    """
    Load input data into staging tables and fill star schema tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Start loading staging tables...\n")
    load_staging_tables(cur, conn)
    print("Start inserting in star schema tables...\n")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()



