from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    
    @apply_defaults
    def __init__(
        self,
        aws_credentials_id = "",
        redshift_conn_id = "",
        table = "",
        s3_bucket = "",
        s3_key = "",
        json_path = "",
        delimiter = ",",
        ignore_headers = 1,
        data_format = "json",
        *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path =json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.data_format = data_format
        
    def execute(self, context):
        # Fetch credentials from Airflow
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if aws_hook is not None and redshift_hook is not None:
            credentials = aws_hook.get_credentials()

            # Resolve the S3 key; expand to include the given year and month
            s3_key = self.s3_key.format(**context)
            s3_path = f"s3://{self.s3_bucket}/{s3_key}"

            # Clear the table if needed
            self.log.info(f"Clearing data from destination Redshift table ({self.table})")
            redshift_hook.run(f"TRUNCATE {self.table}")
    
            self.log.info(f"Ingesting S3 ({s3_path}) to {self.table} [start]")
        
            cmd = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}' JSON '{self.json_path}' COMPUPDATE OFF"
            redshift_hook.run(cmd)
            
            self.log.info(f"Ingesting S3 ({s3_path}) to {self.table} [end]")

    