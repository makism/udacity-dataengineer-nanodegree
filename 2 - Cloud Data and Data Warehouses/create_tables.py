import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import tqdm

# Print some extra "debug" details; this may mess the nice output :-)
DEBUG_VERBOSE = False


def drop_tables(cur, conn):
    """ Drop tables.
    
    It will iterate and execute all the drop-related queries
    found in the list `drop_table_queries`.
    
    Parameters
    ----------
    cur: psycopg2.cursor
        A cursor object to our database.
    
    conn: psycopg2.connection
        A connection/session object.
    """
    
    print("Dropping tables...")
    for idx in tqdm.tqdm(range(len(drop_table_queries))):
        query = drop_table_queries[idx]
        if len(query) > 1:
            if DEBUG_VERBOSE:
                print(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()


def create_tables(cur, conn):
    """ Create tables.
    
    It will iterate and execute all the create-related queries
    found in the list `create_table_queries`.
    
    Parameters
    ----------
    cur: psycopg2.cursor
        A cursor object to our database.
    
    conn: psycopg2.connection
        A connection/session object.
    """
    
    print("Creating tables...")
    for idx in tqdm.tqdm(range(len(create_table_queries))):
        query = create_table_queries[idx]
        if len(query) > 1:
            if DEBUG_VERBOSE:
                print(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()


def main():
    """ The main entrypoint function.
    
    - Creates a connection and a cursor object with our database (given the parameters).
    - Invokes `drop_tables` and `create_tables` to clean up and initialize our database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    
    print("")
    print("Done, bye bye.")


if __name__ == "__main__":
    main()