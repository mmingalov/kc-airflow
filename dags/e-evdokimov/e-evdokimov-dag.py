"""
Get data from greenplum and push to XCom
"""
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime
from datetime import timedelta


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'e-evdokimov',
    'poke_interval': 600
}

with DAG("from_gp_to_xcom",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-evdokimov']) as dag:

    def get_data_from_greenplum_and_push_to_xcom(**kwargs):
        # Create connection to Greenplum
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Get execution weekday
        exec_wd = kwargs['execution_date'].weekday() + 1

        # Get data from greenplum
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_wd}')
        query_res = cursor.fetchone()[0]

        # print to log
        logging.info(f"Current weekday: {exec_wd}")
        logging.info(f"Query result {query_res}")
        logging.info("   ")

        # push data to XCom
        kwargs['ti'].xcom_push(value=query_res, key='heading')


    from_gp_to_xcom = PythonOperator(
        task_id='from_gp_to_xcom',
        python_callable=get_data_from_greenplum_and_push_to_xcom,
        provide_context=True
    )

    dummy = DummyOperator(task_id='starting_script')

    wait_until_6am = TimeDeltaSensor(
        task_id='wait_until_6am',
        delta=timedelta(seconds=6 * 60 * 60),
    )


    wait_until_6am >> dummy >> from_gp_to_xcom