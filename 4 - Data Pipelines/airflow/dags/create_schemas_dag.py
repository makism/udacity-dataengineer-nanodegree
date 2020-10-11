from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers import SqlCreateQueries


def create_schema_query(sql_query):
    """ Run the given sql query.
    
    Parameters
    ----------
    sql_query: string
        An SQL query that creates a schema.
    """
    redshift_hook = PostgresHook("redshift")
    
    if redshift_hook is not None:
        redshift_hook.run(sql_query)

def truncate_tables(tables):
    """ Truncates the give tables.
    
    This is useful when the tables exist, but maybe contain data.
    
    Parameters
    ----------
    tables: list
        A list of tables to be truncated.
    """
    redshift_hook = PostgresHook("redshift")
    
    if redshift_hook is not None:
        for tbl in tables:
            redshift_hook.run(f"TRUNCATE {tbl};")

default_args = {
    "owner": "udacity",
    "start_date": datetime(2020, 9, 24),
    "end_date": datetime(2020, 9, 25),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "catchup": False,
    "email_on_retry": False,
}

dag = DAG("create_schemas_dag",
          default_args=default_args,
          description="Create schemas",
          schedule_interval='@once'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

task_create_staging_events = PythonOperator(
    task_id = "create_staging_events",
    dag=dag,
    provide_context=False,
    python_callable=create_schema_query,
    op_kwargs={"sql_query": SqlCreateQueries.StagingEvents}
)

task_create_staging_songs = PythonOperator(
    task_id = "create_staging_songs",
    dag=dag,
    provide_context=False,
    python_callable=create_schema_query,
    op_kwargs={"sql_query": SqlCreateQueries.StagingSongs}
)

task_create_facts_songplays = PythonOperator(
    task_id = "create_facts_songplays",
    dag=dag,
    provide_context=False,
    python_callable=create_schema_query,
    op_kwargs={"sql_query": SqlCreateQueries.Songplays}
)

task_create_tbl_artists = PythonOperator(
    task_id = "create_table_artists",
    dag=dag,
    provide_context=False,
    python_callable=create_schema_query,
    op_kwargs={"sql_query": SqlCreateQueries.Artists}
)

task_create_tbl_songs = PythonOperator(
    task_id = "create_table_songs",
    dag=dag,
    provide_context=False,
    python_callable=create_schema_query,
    op_kwargs={"sql_query": SqlCreateQueries.Songs}
)

task_create_tbl_time = PythonOperator(
    task_id = "create_table_time",
    dag=dag,
    provide_context=False,
    python_callable=create_schema_query,
    op_kwargs={"sql_query": SqlCreateQueries.Time}
)

task_create_tbl_users = PythonOperator(
    task_id = "create_table_users",
    dag=dag,
    provide_context=False,
    python_callable=create_schema_query,
    op_kwargs={"sql_query": SqlCreateQueries.Users}
)

task_truncate_all = PythonOperator(
    task_id = "truncate_all",
    dag=dag,
    provide_context=False,
    python_callable=truncate_tables,
    op_kwargs={"tables": [ "staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"]}
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [task_create_staging_events, task_create_staging_songs, task_create_tbl_artists, task_create_tbl_songs, task_create_tbl_time, task_create_tbl_users, task_create_facts_songplays]
[task_create_staging_events, task_create_staging_songs, task_create_tbl_artists, task_create_tbl_songs, task_create_tbl_time, task_create_tbl_users, task_create_facts_songplays] >> task_truncate_all
task_truncate_all >> end_operator
