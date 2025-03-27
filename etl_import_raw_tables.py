from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime.now() - timedelta(days=1),
    'catchup': False
}

with DAG(
    dag_id='etl_import_raw_tables',
    default_args=default_args,
    schedule_interval='@daily',
    tags=['snowflake', 'etl']
) as dag:

    create_user_session_table = SnowflakeOperator(
        task_id='create_user_session_table',
        sql="""
            CREATE TABLE IF NOT EXISTS mydatabase.raw.user_session_channel (
                userId INT NOT NULL,
                sessionId VARCHAR(32) PRIMARY KEY,
                channel VARCHAR(32) DEFAULT 'direct'
            );
        """,
        snowflake_conn_id='snowflake_conn'
    )

    create_session_timestamp_table = SnowflakeOperator(
        task_id='create_session_timestamp_table',
        sql="""
            CREATE TABLE IF NOT EXISTS mydatabase.raw.session_timestamp (
                sessionId VARCHAR(32) PRIMARY KEY,
                ts TIMESTAMP
            );
        """,
        snowflake_conn_id='snowflake_conn'
    )

    create_blob_stage = SnowflakeOperator(
        task_id='create_blob_stage',
        sql="""
            CREATE OR REPLACE STAGE mydatabase.raw.blob_stage
            URL = 's3://s3-geospatial/readonly/'
            FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """,
        snowflake_conn_id='snowflake_conn'
    )

    copy_user_session_data = SnowflakeOperator(
        task_id='copy_user_session_data',
        sql="""
            COPY INTO mydatabase.raw.user_session_channel
            FROM @mydatabase.raw.blob_stage/user_session_channel.csv;
        """,
        snowflake_conn_id='snowflake_conn'
    )

    copy_session_timestamp_data = SnowflakeOperator(
        task_id='copy_session_timestamp_data',
        sql="""
            COPY INTO mydatabase.raw.session_timestamp
            FROM @mydatabase.raw.blob_stage/session_timestamp.csv;
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Define task order
    [create_user_session_table, create_session_timestamp_table] >> create_blob_stage
    create_blob_stage >> [copy_user_session_data, copy_session_timestamp_data]
