from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlDataQualityQueries

class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self,
                 tables,
                 redshift_conn_id = "redshift",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if redshift_hook is not None:
            for tbl in self.tables:
                if tbl in SqlDataQualityQueries.queries:
                    test_pass = False

                    self.log.info(f"Running quality checks for table {tbl}.")

                    test = SqlDataQualityQueries.queries[tbl]

                    query = test['test'].format(tbl)
                    result = redshift_hook.get_records(query)
                    num_records = result[0][0]
                    
                    self.log.info(f"Running test: {query}")
                    
                    if "expected" in test:
                        test_pass = num_records == test['expected']
                    elif "not_expected" in test:
                        test_pass = num_records != test['not_expected']
                       
                    print(f"Result: {result}")
                       
                    if test_pass:
                        self.log.info("==== TEST PASS ====")
                    else:
                        self.log.info("==== TEST FAIL ====")
                        
                    self.log.info("")

    
