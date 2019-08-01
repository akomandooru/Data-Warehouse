from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    dim_reload_sql_template = """
        DROP TABLE IF EXISTS {dim_table};
        CREATE TABLE {dim_table} AS
        {selection_sql};
    """
    dim_append_sql_template = """
        INSERT INTO {dim_table}
        {selection_sql};
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 selection_sql="",
                 dim_table="",
                 reload=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.selection_sql = selection_sql
        self.dim_table = dim_table
        self.reload = reload

    def execute(self, context):
        self.log.info('LoadDimensionOperator processing started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.reload:
            dim_sql = LoadDimensionOperator.dim_reload_sql_template.format(
                dim_table=self.dim_table,
                selection_sql=self.selection_sql)
        else:
            dim_sql = LoadDimensionOperator.dim_append_sql_template.format(
                dim_table=self.dim_table,
                selection_sql=self.selection_sql)
        redshift.run(dim_sql)
        self.log.info('LoadDimensionOperator processing completed successfully')
