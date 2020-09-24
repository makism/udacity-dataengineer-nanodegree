""" Airflow setup

A simple script to create the required Airflow connections programatically.
1. AWS connection
2. Postgresql (Redshift) connection
"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>

from airflow import settings
from airflow.models import Connection
import tqdm

if __name__ == "__main__":
    session = settings.Session()
    
    aws_credentials = Connection(
            conn_id="aws_credentials",
            conn_type="aws",
            login="",
            password="",
    )
    
    redshift = Connection(
            conn_id="redshift",
            conn_type="postgresql",
            host="",
            schema="dev",
            login="awsuser",
            password="",
            port=5439
    )
    
    new_conns = [aws_credentials, redshift]
    for conn in tqdm.tqdm(new_conns):
        session.add(conn)
        session.commit()
