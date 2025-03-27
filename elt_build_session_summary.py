from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime.now() - timedelta(days=1),
    'catchup': False
}

with DAG(
    dag_id='elt_build_session_summary',
    default_args=default_args,
    schedule_interval='@daily',
    tags=['snowflake', 'elt']
) as dag:

    build_session_summary = SnowflakeOperator(
        task_id='build_session_summary_table',
        sql="""
            CREATE OR REPLACE TABLE mydatabase.analytics.session_summary AS
            SELECT userId, sessionId, channel, ts AS session_start
            FROM (
                SELECT 
                    s.userId,
                    s.sessionId,
                    s.channel,
                    t.ts,
                    ROW_NUMBER() OVER (PARTITION BY s.sessionId ORDER BY t.ts DESC) AS rn
                FROM mydatabase.raw.user_session_channel s
                JOIN mydatabase.raw.session_timestamp t
                ON s.sessionId = t.sessionId
            )
            WHERE rn = 1;
        """,
        snowflake_conn_id='snowflake_conn'
    )
