from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


#The DAG is scheduled to run once a day at 7 am, email on retry is set to false, retry every 5 minutes for a maximum of 3 times
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2019, 1, 12),
}

#Catchup is turned off
dag = DAG('etl_dag',
          default_args=default_args,
          description='ETL from S3 to Redshift',
          catchup=False,
          schedule_interval='@daily'
        )

#Begin_execution task in the DAG
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Stage_Stock_Price task in the DAG for copying price data from S3 storage to Redshift staging_stock_price table
stage_price_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Stock_Price',
    dag=dag,
    table="staging_stock_price",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="akcapstone",
    s3_key="priceinfo",
    format="csv IGNOREHEADER 1"
)

#Stage_Stock_Information task in the DAG for copying stock description data from S3 storage to Redshift staging_stock_info table
stage_info_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Stock_Information',
    dag=dag,
    table="staging_stock_info",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="akcapstone",
    s3_key="descinfo",
    format="json 'auto'"
)

#Load_fact_stock_price_table task in the DAG to load data from the staging tables to the fact_stock_price fact table
load_price_fact_table = LoadFactOperator(
    task_id='Load_fact_stock_price_table',
    dag=dag,
    redshift_conn_id="redshift",
    selection_sql=SqlQueries.fact_stock_price_table_insert,
    fact_table='fact_stock_price'
)

#load stock industry dimension table
load_industry_dim_table = LoadDimensionOperator(
    task_id='Load__dim_stock_industry_table',
    dag=dag,
    redshift_conn_id="redshift",
    selection_sql="""SELECT distinct industry, ticker  FROM staging_stock_info""",
    dim_table='dim_stock_industry',
    reload=True
)

#load stock exchange dimension table
load_exchange_dim_table = LoadDimensionOperator(
    task_id='Load_dim_stock_exchange_table',
    dag=dag,
    redshift_conn_id="redshift",
    selection_sql="""SELECT distinct exchange, ticker FROM staging_stock_info""",
    dim_table='dim_stock_exchange',
    reload=True
)

#load info dimension table
load_info_dim_table = LoadDimensionOperator(
    task_id='Load_dim_stock_information_table',
    dag=dag,
    redshift_conn_id="redshift",
    selection_sql="""SELECT distinct name, ticker FROM staging_stock_info""",
    dim_table='dim_stock_information',
    reload=True
)
#load date dimension table
load_date_dim_table = LoadDimensionOperator(
    task_id='Load_dim_stock_date_table',
    dag=dag,
    redshift_conn_id="redshift",
    selection_sql="""SELECT distinct date as date, cast(DATE_PART(y,date) as integer) as year, cast(DATE_PART(mon,date) as integer) as month,cast(DATE_PART(d,date) as integer) as day FROM staging_stock_price""",
    dim_table='dim_stock_date',
    reload=True
)

#load sector dimension table
load_sector_dim_table = LoadDimensionOperator(
    task_id='Load_dim_stock_sector_table',
    dag=dag,
    redshift_conn_id="redshift",
    selection_sql="""SELECT distinct sector, ticker FROM staging_stock_info""",
    dim_table='dim_stock_sector',
    reload=True
)

#Data quality check function that fails if a table is empty (zero records)
def check_greater_than_zero(redshift_hook,table,logging):
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Has records check failed. {table} returned no results")
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError(f"Has records check failed. {table} contained 0 rows")
    logging.info(f"Has records check on table {table} passed with {records[0][0]} records")

#Data quality check function that checks if fact_stock_price record count is over million
def check_greater_than_equal_million(redshift_hook,table,logging):
    if table == 'fact_stock_price':
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Has million records check failed. {table} returned no results")
        num_records = records[0][0]
        if num_records < 1000000:
            raise ValueError(f"Has million records check failed. {table} {num_records} contained less than 1000000 rows")
        logging.info(f"Has million records check on table {table} passed with {records[0][0]} records")

#Quality check operator that performs two checks on the fact and dimension tables
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['fact_stock_price','dim_stock_industry','dim_stock_exchange','dim_stock_information','dim_stock_date','dim_stock_sector'],
    check_functions=[check_greater_than_zero,check_greater_than_equal_million]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#sequence the tasks for dependency
start_operator >> stage_price_to_redshift
start_operator >> stage_info_to_redshift
stage_price_to_redshift >> load_price_fact_table
stage_info_to_redshift >> load_industry_dim_table
load_price_fact_table >> load_date_dim_table
stage_info_to_redshift >> load_sector_dim_table
stage_info_to_redshift >> load_info_dim_table
stage_info_to_redshift >> load_exchange_dim_table
load_industry_dim_table >> run_quality_checks
load_date_dim_table >> run_quality_checks
load_sector_dim_table >> run_quality_checks
load_info_dim_table >> run_quality_checks
load_exchange_dim_table >> run_quality_checks
load_price_fact_table >> run_quality_checks
run_quality_checks >> end_operator
