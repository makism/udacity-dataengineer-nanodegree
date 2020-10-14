""" Airflow setup

A simple script to create the required Airflow connections programatically.
1. AWS connection
2. Postgresql (Redshift) connection
"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>

import sys
sys.path.append("../config")
from config import logger

from airflow import settings
from airflow.models import Connection
import json
import tqdm


def create_aws_credentials(dwh):
    """ Creates and configures an AWS connection.
    
    
    Parameters
    ----------
    dwh: dict
        A dictionary with the required parameters.
    
    Returns
    -------
    conn: airflow.models.Connection
        An instance of airflow.models.Connection.
    """
    conn = Connection(
        conn_id="aws_credentials",
        conn_type="aws",
        login=dwh['aws']['access_key_id'],
        password=dwh['aws']['secret_access_key'],
    )
    
    return conn

def create_redshift_connection(dwh):
    """ Creates and configures a Redshift connection.
    
    
    Parameters
    ----------
    dwh: dict
        A dictionary with the required parameters.
    
    Returns
    -------
    conn: airflow.models.Connection
        An instance of airflow.models.Connection.
    """
    conn = Connection(
        conn_id="redshift",
        conn_type="postgresql",
        host=dwh['redshift']['host'],
        schema=dwh['redshift']['db_name'],
        login=dwh['redshift']['db_user'],
        password=dwh['redshift']['db_pass'],
        port=dwh['redshift']['db_port']
    )
    
    return conn

if __name__ == "__main__":
    log_handler = logger.get_logger(logger_name="airflow")
    
    try:
        dwh = None
        
        with open("dwh.json") as fp:
            dwh = json.load(fp)
            log_handler.DEBUG(dwh)
            
        session = settings.Session()
        aws_credentials = create_aws_credentials()
        redshift = create_redshift_connection()

        new_conns = [aws_credentials, redshift]
        for conn in tqdm.tqdm(new_conns):
            session.add(conn)
            session.commit()
            
    except Exception as err:
        log_handler.error(err)
