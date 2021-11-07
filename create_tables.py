import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tales in the drop_tables_queries list, found in sql_queries.py
    cur: The cursor to the database connection.
    conn: The connection objct to the database.
    """
    for count, query in enumerate(drop_table_queries):
        print(f'[{count+1} of {len(drop_table_queries)} in drop_table_queries] Executing query...')
        try:
            cur.execute(query)
        except Exception as e:
            print(f'[{count+1} of {len(drop_table_queries)} in drop_table_queries] There was an error executing this query. {e}')
        finally:
            conn.commit()
            print(f'[{count+1} of {len(drop_table_queries)} in drop_table_queries] Execution complete.')


def create_tables(cur, conn):
    """
    Creates all tales in the create_table_queries list, found in sql_queries.py
    cur: The cursor to the database connection.
    conn: The connection objct to the database.
    """
    for count, query in enumerate(create_table_queries):
        print(f'[{count+1} of {len(create_table_queries)} in create_table_queries] Executing query...')
        try:
            cur.execute(query)
        except Exception as e:
            print(f'[{count+1} of {len(create_table_queries)} in create_table_queries] There was an error executing this query. {e}')
        finally:
            conn.commit()
            print(f'[{count+1} of {len(create_table_queries)} in create_table_queries] Execution complete.')



def main():
    """
    1. Establishes a connection to the database.
    2. Drops tables in that database if they exist.
    3. Creates tables in that database if they don't exist.
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
    
    print('Dropping tables...')
    
    drop_tables(cur, conn)

    print('Dropping tables complete.\n')
    print('Starting create table queries...')
    
    create_tables(cur, conn)

    print('Create tables complete.\n')

    conn.close()
    
    print('Connection closed.')


if __name__ == "__main__":
    main()