from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_functions=[],
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.check_functions = check_functions

    def execute(self, context):
        self.log.info(f'Data quality checks started')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            for check in self.check_functions:
                check(redshift_hook,table,self.log)
        self.log.info(f"Data quality checks completed successfully")
