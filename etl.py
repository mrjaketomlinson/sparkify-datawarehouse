import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 buckets to the staging tables in the copy_table_queries 
        list, found in sql_queries.py
    cur: The cursor to the database connection.
    conn: The connection objct to the database.
    """
    for count, query in enumerate(copy_table_queries):
        print(f'[{count+1} of {len(copy_table_queries)} in copy_table_queries] Executing query...')
        try:
            cur.execute(query)
        except Exception as e:
            print(f'[{count+1} of {len(copy_table_queries)} in copy_table_queries] There was an error executing this query. {e}')
        finally:
            conn.commit()
            print(f'[{count+1} of {len(copy_table_queries)} in copy_table_queries] Execution complete.')


def insert_tables(cur, conn):
    """
    Loads data from the staging tables to the fact/dimension tables using 
        queries in the insert_table_queries list, found in sql_queries.py
    cur: The cursor to the database connection.
    conn: The connection objct to the database.
    """
    for count, query in enumerate(insert_table_queries):
        print(f'[{count+1} of {len(insert_table_queries)} in insert_table_queries] Executing query...')
        try:
            cur.execute(query)
        except Exception as e:
            print(f'[{count+1} of {len(insert_table_queries)} in insert_table_queries] There was an error executing this query. {e}')
        finally:
            conn.commit()
            print(f'[{count+1} of {len(insert_table_queries)} in insert_table_queries] Execution complete.')


def main():
    """
    NOTE: Must run create_tables.py first, in order to create the databse tables
        we are inserting using this script.
    1. Establishes a connection to the database.
    2. Loads data from S3 buckets to the staging tables.
    3. Inserts data from the staging tables to fact and dimension tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:
        print('Establing connection with redshift...')
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print('Connection established.\n')
    except Exception as e:
        print(f'There was an error connecting to the database. {e}')
        return None
    
    print('Starting staging table load...')
    
    load_staging_tables(cur, conn)
    
    print('Staging table load finished.\n')
    print('Starting insert queries...')
    
    insert_tables(cur, conn)
    
    print('Insert queries complete.\n')

    conn.close()

    print('Connection closed.')


if __name__ == "__main__":
    main()