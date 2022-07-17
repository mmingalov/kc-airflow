"""
Test dag
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'Izrantsev',
    'poke_interval': 600
}

with DAG("izrantsev_dag_test",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['izrantsev']
         ) as dag:
    start = DummyOperator(task_id='start')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command="echo {{ a-gajdabura }}",
    )


    def get_weekday_2(**kwargs):
        from dateutil import parser
        wd = parser.isoparse(kwargs['a-gajdabura']).isoweekday()
        kwargs['ti'].xcom_push(value=wd, key='weekday')


    weekday_2 = PythonOperator(
        task_id='weekday_2',
        python_callable=get_weekday_2,
    )


    def get_weekday(**kwargs):
        import sys
        from dateutil import parser
        wd = parser.isoparse(kwargs['a-gajdabura']).isoweekday()
        dis = kwargs['data_interval_start']
        #dis_wd = parser.isoparse(dis).isoweekday()
        #dis_wd2 = str(1 + int(dis_wd))
        xcom = kwargs['ti'].xcom_pull(task_ids='weekday_2', key='weekday')
        logging.info('\n--------------------------------------------------------\n')
        logging.info('**** Python version is: {} ****'.format(sys.version_info))
        logging.info('**** Parsed from dateutil WeekDay is: {} ****'.format(wd))
        logging.info('**** Dis.isoweekday() is: {} ****'.format(kwargs['dis']))
        logging.info('**** Dis.isoweekday() is: {} ****'.format(dis))
        #logging.info('**** Dis.isoweekday() is: {} ****'.format(dis_wd))
        #logging.info('**** Dis.isoweekday() is: {} ****'.format(dis_wd2))
        logging.info('**** XCOM WeekDay is: {} ****'.format(xcom))
        logging.info("**** kwargs['a-gajdabura'] is: {} ****".format(kwargs['a-gajdabura']))
        logging.info('**** Dis.weekday() is: {} ****'.format(kwargs['dis2']))
        logging.info('**** Dis.weekday() is: {} ****'.format(type(kwargs['dis2'])))
        logging.info('\n--------------------------------------------------------\n')


    weekday = PythonOperator(
        task_id='weekday',
        python_callable=get_weekday,
        op_kwargs={'execution_dt': '{{ a-gajdabura }}',
                   'dis': '{{ data_interval_start.isoweekday() }}',
                   'dis2': '{{ data_interval_start.weekday() }}',
                   },

    )


    def gp_getdata(**kwargs):
        from dateutil import parser
        wd = parser.isoparse(kwargs['ds_str']).isoweekday()
        hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {wd}')
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(value=query_res, key='result')
        logging.info('\n--------------\n')
        logging.info(query_res)
        logging.info('\n--------------\n')


    postgres = PythonOperator(
        task_id='postgres',
        python_callable=gp_getdata,
        op_kwargs={'ds_str': '{{ a-gajdabura }}',
                   },
    )


    def hello_man_func():
        logging.info("Hello man!")


    hello_man = PythonOperator(
        task_id='hello_pipl',
        python_callable=hello_man_func
    )

start >> [echo_ds >> weekday, postgres, hello_man]
