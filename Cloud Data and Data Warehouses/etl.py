import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import tqdm

# Print some extra "debug" details; this may mess the nice output :-)
DEBUG_VERBOSE = False


def load_staging_tables(cur, conn):
    """ Copy S3 Buckets to staging tables.
    
    Parameters
    ----------
    cur: psycopg2.cursor
        A cursor object to our database.
    
    conn: psycopg2.connection
        A connection/session object.
    """
    
    for idx in tqdm.tqdm(range(len(copy_table_queries))):
        query = copy_table_queries[idx]
        if len(query) > 1:
            if DEBUG_VERBOSE:
                print(query)
            cur.execute(query)
            conn.commit()


def insert_tables(cur, conn):
    """ 
    
    Parameters
    ----------
    cur: psycopg2.cursor
        A cursor object to our database.
    
    conn: psycopg2.connection
        A connection/session object.
    """
    
    for idx in tqdm.tqdm(range(len(insert_table_queries))):
        query = insert_table_queries[idx]
        if len(query) > 1:
            if DEBUG_VERBOSE:
                print(query)
            cur.execute(query)
            conn.commit()


def main():
    """ The main entrypoint function.
    
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()