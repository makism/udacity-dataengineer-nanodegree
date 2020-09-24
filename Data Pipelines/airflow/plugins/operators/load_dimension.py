from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"
    
    @apply_defaults
    def __init__(self,
                 table,
                 sql_query,
                 redshift_conn_id = "redshift",
                 truncate = False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.append = False
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if redshift_hook is not None:
            
            # Whether or not clear the table ;)
            if not self.append:
                self.log.info(f"Truncate table ({self.table})")
                redshift_hook.run(f"TRUNCATE {self.table};")

            self.log.info(f"Loading dimension table {self.table} [start]")
            q = f"""
                BEGIN;
                    INSERT INTO {self.table}
                    {self.sql_query}; 
                COMMIT;
            """
            
            self.log.info(q)

            redshift_hook.run(q)
            
            self.log.info(f"Loading dimension table {self.table} [end]")

