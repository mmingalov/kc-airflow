from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from contextlib import ExitStack
from airflow import macros
from dataclasses import astuple
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from vpismennyj.vpismennyj_ram_max_res_loc import RAM_max_res_loc

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'v.pismennyj-2',
    'poke_interval': 600
}
with DAG(
    "v.pysmennyi_dag3",
    schedule_interval="0 0 * * 1-6",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["v.pismennyj-2"],
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def write_ram_max_res_loc(**kwags):
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        values = kwags["ti"].xcom_pull(task_ids="ram_max", key="max")
        with ExitStack() as estack:
            conn = estack.enter_context(pg_hook.get_conn())
            cursor = estack.enter_context(conn.cursor())
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS
                v_pismennyj_2_ram_location(
                    id integer primary key,
                    name varchar(50),
                    type varchar(30),
                    dimension varchar(30),
                    resident_cnt integer
                    )
                """
            )
            cursor.execute("TRUNCATE v_pismennyj_2_ram_location")
            logging.info(values)
            sql = f"""
                INSERT INTO v_pismennyj_2_ram_location (id, name, type, dimension, resident_cnt)
                VALUES (%s, %s, %s, %s, %s);
                """
            val = [(values[0].id, values[0].name, values[0].type, values[0].dimension, values[0].resident_cnt),
                   (values[1].id, values[1].name, values[1].type, values[1].dimension, values[1].resident_cnt),
                   (values[2].id, values[2].name, values[2].type, values[2].dimension, values[2].resident_cnt)]
            cursor.executemany(sql, val)
            cursor.execute(
                f"""
                SELECT * FROM v_pismennyj_2_ram_location
                """
            )
            result_set = cursor.fetchall()
            logging.info(result_set)


    ram_max = RAM_max_res_loc(task_id="ram_max")
    write_max = PythonOperator(task_id="write_max", python_callable=write_ram_max_res_loc)

    start >> ram_max >> write_max >> end