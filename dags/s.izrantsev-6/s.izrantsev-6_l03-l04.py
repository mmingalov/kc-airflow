"""
Гутен даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'Izrantsev',
    'poke_interval': 600
}

with DAG("izrantsev_dag_hw",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['izrantsev']
         ) as dag:

    start = DummyOperator(task_id='start')

    def get_weekday(**kwargs):
        from dateutil import parser
        wd = parser.isoparse(kwargs['a-gajdabura']).isoweekday()
        kwargs['ti'].xcom_push(value=wd, key='wd')
        logging.info('\n--------------------------------------------------------\n')
        logging.info('**** a-gajdabura is {}, parsed WeekDay is: {} ****'.format(kwargs['a-gajdabura'], wd))
        logging.info('\n--------------------------------------------------------\n')


    weekday = PythonOperator(
        task_id='weekday',
        python_callable=get_weekday,
    )

    def gp_query(**kwargs):
        wd = kwargs['ti'].xcom_pull(task_ids='weekday', key='wd')
        hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {wd}')
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(value=query_res, key='result')
        logging.info('\n--------------------------------------------------------\n')
        logging.info('**** wd from weekday task is {} ****'.format(wd))
        logging.info('**** Result of query is: {} ****'.format(query_res))
        logging.info('\n--------------------------------------------------------\n')


    gp_query = PythonOperator(
        task_id='gp_query',
        python_callable=gp_query,
        op_kwargs={'ds_str': '{{ a-gajdabura }}',
                   },
    )

    end = DummyOperator(task_id='end')

start >> weekday >> gp_query >> end
