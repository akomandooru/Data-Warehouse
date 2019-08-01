from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    fact_sql_template = """
        DROP TABLE IF EXISTS {fact_table};
        CREATE TABLE {fact_table} AS
        {selection_sql};
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 selection_sql="",
                 fact_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.selection_sql = selection_sql
        self.fact_table = fact_table

    def execute(self, context):
        self.log.info('LoadFactOperator execution started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        facts_sql = LoadFactOperator.fact_sql_template.format(
            fact_table=self.fact_table,
            selection_sql=self.selection_sql
        )
        redshift.run(facts_sql)
        self.log.info('LoadFactOperator execution completed successfully')
