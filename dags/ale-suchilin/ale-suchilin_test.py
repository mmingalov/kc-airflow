"""
Даг для домашнего задания
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import timedelta, datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.time_delta_sensor import TimeDeltaSensor

from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'suchilin',
    'email': ['a.souchilin@yandex.ru'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 2, 28),
    'end_date': datetime(2022, 3, 14),
    'poke_interval': 600,
    'catchup':True,
    'trigger_rule':  'one_success'
}

with DAG("ale-suchilin_test",
    schedule_interval='0 10 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ale-suchilin']
) as dag:

    dummy = DummyOperator(
        task_id="dummy",
        start_date= datetime(2022, 2, 28),
        end_date= datetime(2022, 3, 14)
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        start_date= datetime(2022, 2, 28),
        end_date= datetime(2022, 3, 14)
    )

    def get_pass_func():
        try:
            logging.info(BaseHook.get_connection('conn_greenplum').password)
        except Exception as err:
            return False

    get_pass = PythonOperator(
        task_id='get_pass',
        python_callable=get_pass_func
    )

    waitfor1min = TimeDeltaSensor(
        task_id='waitfor1min',
        delta=timedelta(minutes=1),
        start_date=datetime(2022, 2, 28),
        end_date=datetime(2022, 3, 14),
        mode='reschedule'
    )

    def end_of_dag():
        if get_pass_func():
            return 'get_pass'
        return 'echo_ds'

    choice = BranchPythonOperator(
        task_id='choice',
        python_callable=end_of_dag,
        start_date=datetime(2022, 2, 28),
        end_date=datetime(2022, 3, 14)
    )
    send_email = EmailOperator(
        task_id='send_email',
        to='a.souchilin@yandex.ru',
        subject='DAG complete',
        html_content="Date: {{ ds }}"
    ) #Airflow не подключен к smtp, поэтому пришлось убрать таск


    def load_from_greenplum_func(ds):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        #exec_date = '{{ ds }}'
        print(ds)
        exec_day = datetime.strptime(ds, '%Y-%m-%d').weekday()
        cursor.execute('SELECT heading FROM articles WHERE id = %s',[exec_day])  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]
        return query_res


    load_from_greenplum = PythonOperator(
        task_id='load_from_greenplum',
        python_callable=load_from_greenplum_func,
        start_date=datetime(2022, 2, 28),
        end_date=datetime(2022, 3, 14),
        provide_context = True
    )

    task_end = DummyOperator(task_id='task_end')

    dummy >> waitfor1min >> choice >> [echo_ds, get_pass] >> load_from_greenplum >> task_end
