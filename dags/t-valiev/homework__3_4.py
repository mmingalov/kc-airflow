"""
Даг делает всякие штуки
"""
import logging
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


DEFAULT_ARGS = {
    "start_date": days_ago(7),
    "owner": "t-valiev",
}

dag_id = Path(__file__).parent.stem + "__" + Path(__file__).stem

with DAG(
    dag_id=dag_id,
    schedule_interval="0 12 * * 1-6",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["t-valiev"],
) as dag:

    dummy_task = DummyOperator(task_id="dummy")

    bash_kek_task = BashOperator(
        task_id="bash_kek",
        bash_command="echo execution date is {{ execution_date }}, weekday {{ execution_date.weekday() + 1 }}",
    )

    python_kek_task = PythonOperator(
        task_id="python_kek",
        python_callable=lambda: logging.info("privetiki"),
    )

    def get_heading_by_weekday_func(**kwargs):
        pg_hook = PostgresHook(
            postgres_conn_id="conn_greenplum",
        )
        cursor = pg_hook.get_cursor()

        SQL_REQUEST = """
        select heading 
        from articles 
        where id = {weekday};
        """.format(
            weekday=kwargs["templates_dict"]["weekday"]
        )
        cursor.execute(SQL_REQUEST)
        return cursor.fetchone()[0]

    get_heading_by_weekday = PythonOperator(
        task_id="get_heading_by_weekday",
        python_callable=get_heading_by_weekday_func,
        templates_dict={"weekday": "{{ execution_date.weekday() + 1 }}"},
    )

    dummy_task >> bash_kek_task >> python_kek_task >> get_heading_by_weekday
