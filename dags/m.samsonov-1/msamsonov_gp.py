import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "msamsonov",
    "start_date": days_ago(2),
}

with DAG(
    "msamsonov_greenplum",
    schedule_interval="0 0 * * 1-6",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["msamsonov"],
) as dag:

    def save_heading_func(**context):
        weekday = context["execution_date"].weekday() + 1
        logging.info(f"weekday: {weekday}")

        # just in case
        if weekday == 7:
            return

        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")
        conn = pg_hook.get_conn()

        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f"SELECT heading FROM articles WHERE id = {weekday}")
        heading = cursor.fetchone()[0]
        logging.info(f"heading: {heading}")

        context["ti"].xcom_push(value=heading, key="msamsonov_article_heading")

    save_heading = PythonOperator(
        task_id="save_heading", provide_context=True, python_callable=save_heading_func
    )
