import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'a-beljaev-3',
    'poke_interval': 600
}

with DAG("a-beljaev-3",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-beljaev-3']
         ) as dag:

    echo_yesterday_ds = BashOperator(
        task_id='echo_yesterday_ds',
        bash_command='echo "Now: {{ ts }}"',
        dag=dag
    )

    dummy = DummyOperator(task_id='dummy',
                          trigger_rule='all_done')

    def get_data_from_greenplum_and_push_to_xcom(**kwargs):
        # Create connection to Greenplum
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Get weekday
        wd = datetime.datetime.now().weekday()+1  # monday = 0 but in Greenplum ID starts from 1

        # Get data from database
        cursor.execute(f'SELECT heading FROM articles WHERE id = {wd}')
        heading_string = cursor.fetchall()

        # Logging result
        logging.info('--------')
        logging.info(f'Weekday: {wd}')
        logging.info(f'Return value: {heading_string}')
        logging.info('--------')

        # Write data to XCOM
        kwargs['ti'].xcom_push(value=heading_string, key='heading')

    push_to_xcom = PythonOperator(
        task_id='push_to_xcom',
        python_callable=get_data_from_greenplum_and_push_to_xcom,
        provide_context=True
    )

    echo_yesterday_ds >> dummy >> push_to_xcom
