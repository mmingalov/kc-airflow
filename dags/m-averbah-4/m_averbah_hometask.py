from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.sensors.weekday import DayOfWeekSensor

from datetime import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-averbah-4',
    'poke_interval': 600
}

with DAG("m_averbah_hometask",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['mahw']
) as dag:

    start = DummyOperator(task_id="start")

    """" функция определяет четный или нечетный день"""
    def even_odd_func(execution_date):
        exec_day=datetime.strptime(execution_date, '%Y-%m-%d').day
        if exec_day%2==0:
            return ['echo_even']
        return['echo_odd']

    """если день четный вызывается функция, печатающая 'even', если нечетный 'odd'"""
    even_odd = BranchPythonOperator(
        task_id='even_odd',
        python_callable=even_odd_func,
        op_kwargs={'execution_date': '{{a-gajdabura}}'}
    )

    echo_even = BashOperator(
        task_id='echo_even',
        bash_command='echo even'
    )

    echo_odd = BashOperator(
        task_id='echo_odd',
        bash_command='echo even'
    )

    """функция определяет день недели и отправляет его в XCOM"""
    def day_of_week_func(execution_date):
        day_of_week = datetime.strptime(execution_date, '%Y-%m-%d').weekday()+1
        return day_of_week

    day_of_week=PythonOperator(
        task_id='day_of_week',
        python_callable = day_of_week_func,
        op_kwargs = {'execution_date': '{{a-gajdabura}}'}
    )

    """функция читает из greenplum и загружает в XCOM"""
    def load_from_greenplum_func(**kwargs):
        day_of_week=kwargs['templates_dict']['implicit']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week} ')
        query_res = cursor.fetchall()
        return query_res[0][0]

    load_from_greenplum=PythonOperator(
        task_id='load_from_greenplum',
        python_callable=load_from_greenplum_func,
        templates_dict = {'implicit': '{{ ti.xcom_pull(task_ids="day_of_week") }}'},
        provide_context = True
    )

    """вывод результата в лог"""
    def print_result_func(**kwargs):
        logging.info('--------------')
        logging.info(kwargs['templates_dict']['implicit'])
        logging.info('--------------')

    print_result = PythonOperator(
        task_id='print_result',
        python_callable=print_result_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="load_from_greenplum") }}'},
        provide_context=True
    )

    start >> [even_odd, day_of_week]
    even_odd >> [echo_even, echo_odd]
    day_of_week >> load_from_greenplum >> print_result