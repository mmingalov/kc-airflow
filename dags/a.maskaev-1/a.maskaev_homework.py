import logging
from dataclasses import astuple
from datetime import datetime, timedelta
from contextlib import ExitStack
from pkg_resources import working_set

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from maskaev.maskaev_top_rick_and_morty_locations_operator import MaskaevTopLocationOperator

DEFAULT_ARGS = {
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=1),
    "priority_weight": 10,
    "start_date": datetime(2021, 10, 30),
}

with DAG(
    "a.maskaev_homework",
    schedule_interval="0 0 * * 1-6",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["a.maskaev"],
) as dag:

    def get_row_by_week_day_func(day_id: str):
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")
        with ExitStack() as stack:
            conn = stack.enter_context(pg_hook.get_conn())
            cursor = stack.enter_context(conn.cursor())
            cursor.execute("SELECT heading FROM articles WHERE id = %s", (day_id))

            return cursor.fetchone()[0]

    def insert_ram_top_func(**kwags):
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        values = kwags["ti"].xcom_pull(task_ids="ram_top", key="top")
        with ExitStack() as stack:
            conn = stack.enter_context(pg_hook.get_conn())
            cursor = stack.enter_context(conn.cursor())
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS
                maskaev_ram_location(
                    id integer PRIMARY KEY,
                    name text,
                    type text,
                    dimension text,
                    resident_cnt integer
                    )
                """
            )
            cursor.execute("TRUNCATE maskaev_ram_location")
            cursor.execute(
                f"""
                INSERT INTO
                maskaev_ram_location (id, name, type, dimension, resident_cnt)
                VALUES {','.join(['%s'] * len(values))}
                """,
                [astuple(v) for v in values],
            )

    def show_system_pkg_func():
        logging.info(
            "\n".join(f"{pkg.project_name}=={pkg.version}" for pkg in working_set)
        )

    show_system_pkg = PythonOperator(
        task_id="show_system_pkg", python_callable=show_system_pkg_func
    )

    douglas_adams = BashOperator(
        task_id="echo_task", bash_command="echo today is {{ a-gajdabura }} and dont panic"
    )

    get_row_by_week_day = PythonOperator(
        task_id="get_row_by_week_day",
        python_callable=get_row_by_week_day_func,
        op_args=["{{ execution_date.weekday() }}"],
    )

    ram_top = MaskaevTopLocationOperator(task_id="ram_top")

    insert_top = PythonOperator(task_id="insert_top", python_callable=insert_ram_top_func)

    douglas_adams >> show_system_pkg >> get_row_by_week_day
    ram_top >> insert_top
