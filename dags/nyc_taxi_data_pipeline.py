import os
import logging
import pathlib
import pandas as pd
import pyarrow.parquet as pq
from time import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain


SQL_FILE_PATH = os.environ.get("SQL_FILE_PATH", f"{ pathlib.Path(__file__).resolve().parents[2] }/include/sql")
                                                 
PG_CONN_ID = "postgres_conn"
PG_DB_NAME = "airflow"
PG_SCHEMA_NAME = "stg"  

# URLs for both taxi types
YELLOW_TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
GREEN_TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet"
TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Table names
YELLOW_TABLE_NAME = "yellow_tripdata"
GREEN_TABLE_NAME = "green_tripdata"
TAXI_ZONE_LOOKUP_TABLE_NAME = "taxi_zone_lookup"

SCHEMA_MIGR = ["create_schemas", "create_tables"]


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 8, tzinfo=timezone.utc),
    "email": ["qudus.ayoola123@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "params": {
        "": ""
    }
}


# Main task for data ingestion
@task()
def ingest_taxi_data(url: str, tb: str, database: str, schema: str, taxi_type: str):
    logging.info(f"Starting {taxi_type} taxi data ingestion task.")
    
    # Extract file name from URL
    file_name = url.rsplit('/', 1)[-1].strip()
    logging.info(f'Downloading {taxi_type} taxi file from {url} as {file_name}')
    
    os.system(f'curl {url.strip()} -o {file_name}')
    logging.info(f'{taxi_type} taxi download completed.')

    # Use Airflow's PostgresHook
    logging.info(f'Creating Postgres hook with conn ID: {PG_CONN_ID}')
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID, schema=database)  
    engine = pg_hook.get_sqlalchemy_engine()

    # Read file based on file type
    if '.csv' in file_name:
        logging.info(f'Reading CSV file: {file_name}')
        df = pd.read_csv(file_name, nrows=10)
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=300000)
    elif '.parquet' in file_name:
        logging.info(f'Reading Parquet file: {file_name}')
        parquet_file = pq.ParquetFile(file_name)
        df = next(parquet_file.iter_batches(batch_size=10)).to_pandas()
        df_iter = parquet_file.iter_batches(batch_size=300000)
    else:
        raise ValueError("Only .csv or .parquet files are supported.")

    # Insert data in batches
    t_start = time()
    count = 0
    for batch in df_iter:
        count += 1
        batch_df = batch.to_pandas() if '.parquet' in file_name else batch

        batch_df.columns = [c.lower() for c in batch_df.columns]

        logging.info(f'Inserting {taxi_type} batch {count} into {schema}.{tb}...')
        b_start = time()
        batch_df.to_sql(name=tb, con=engine, schema=schema, if_exists='append', index=False)
        b_end = time()
        logging.info(f'Inserted {taxi_type} batch {count} in {b_end - b_start:.3f} seconds.')

    t_end = time()
    logging.info(f'All {taxi_type} batches inserted into {schema}.{tb}. Total time: {t_end - t_start:.3f} seconds for {count} batches.')

def split_queries(query: str):
    queries = query.strip().split(';')
    stripped_queries = list(map(str.strip, queries))
    return list(filter(lambda x: x, stripped_queries))


def get_postgres_query_operator(
    task_id: str,
    query: str,
    database: str = PG_DB_NAME,
    postgres_conn_id: str = PG_CONN_ID,
    push_xcom: bool = False,
    params: dict = {}
):
    queries = split_queries(query)
    return PostgresOperator(
        task_id=task_id,
        database=database,
        sql=queries,
        postgres_conn_id=postgres_conn_id,
        do_xcom_push=push_xcom,
        depends_on_past=False,
        retries=default_args["retries"],
        retry_delay=default_args["retry_delay"],
        parameters=params
    )

@task()
def start():
    logging.info("Starting DAG Run")

@task()
def end():
    logging.info("DAG Run Completed")

# Define DAG
@dag(
    dag_id="load_taxi_data_to_postgres",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["taxi", "postgres", "etl"]
)
def load_taxi_data_to_postgres():
    start_task = start()
    end_task = end()

    with TaskGroup(group_id="schema_migration") as schema_migration:
        create_schemas = get_postgres_query_operator(
            "create_schemas",
            open(f'{SQL_FILE_PATH}/schema_migration/create_schemas.sql').read()
        )
        create_tables = get_postgres_query_operator(
            "create_tables",
            open(f'{SQL_FILE_PATH}/schema_migration/create_tables.sql').read()
        )
        create_schemas >> create_tables

    with TaskGroup(group_id="data_ingestion") as data_ingestion:
        # Yellow taxi data ingestion
        ingest_yellow_data = ingest_taxi_data.override(task_id="load_yellow_taxi_data")(
            url=YELLOW_TAXI_URL, 
            tb=YELLOW_TABLE_NAME, 
            database=PG_DB_NAME, 
            schema=PG_SCHEMA_NAME,
            taxi_type="yellow"
        )
        
        # Green taxi data ingestion
        ingest_green_data = ingest_taxi_data.override(task_id="load_green_taxi_data")(
            url=GREEN_TAXI_URL, 
            tb=GREEN_TABLE_NAME, 
            database=PG_DB_NAME, 
            schema=PG_SCHEMA_NAME,
            taxi_type="green"
        )

        # Green taxi data ingestion
        ingest_taxi_zone_lookup = ingest_taxi_data.override(task_id="load_taxi_zone_lookup")(
            url=GREEN_TAXI_URL, 
            tb=GREEN_TABLE_NAME, 
            database=PG_DB_NAME, 
            schema=PG_SCHEMA_NAME,
            taxi_type="green"
        )
        
        # Both ingestion tasks can run in parallel
        [ingest_yellow_data, ingest_green_data, ingest_taxi_zone_lookup]

    with TaskGroup(group_id="data_transformation") as data_transformation:
        transf_dim_tables = get_postgres_query_operator(
            "transf_dim_tables",
            open(f'{SQL_FILE_PATH}/transformation/dim_tables.sql').read()
        )
        transf_fact_tables = get_postgres_query_operator(
            "transf_fact_tables",
            open(f'{SQL_FILE_PATH}/transformation/fact_tables.sql').read()
        )
        transf_dim_tables >> transf_fact_tables

    start_task >> schema_migration >> data_ingestion >> data_transformation >> end_task

dag = load_taxi_data_to_postgres()