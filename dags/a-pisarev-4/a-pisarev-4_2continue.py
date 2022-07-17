"""
 DAG
 triggered by another DAG

 Here we have tasks for lesson 4.

"""
import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'a-pisarev-4',
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'poke_interval': 600,
    'trigger_rule': 'all_success'
}

with DAG(
    dag_id='a-pisarev-4_2continue_DAG',
    schedule_interval='@daily',     # '*/10 * * * *',       # ..every 10 minutes    '@hourly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-pisarev-4']
) as dag:

    start_task = ExternalTaskSensor(
        task_id='start_task',
        external_dag_id='a-pisarev-4_1starting_DAG',
        external_task_id='end_task',
        timeout=300,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule'
    )

    def explicit_push_func(**kwargs):
        kwargs['ti'].xcom_push(value='Hello world!', key='hi')

    def func_get_weekday_id(execution_dt):
        weekday_id = datetime.datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return weekday_id

    def func_is_not_sunday(execution_dt):
        return func_get_weekday_id(execution_dt) not in [6]

    no_sunday_task = ShortCircuitOperator(
        task_id='no_sunday_task',
        python_callable=func_is_not_sunday,
        op_kwargs={'execution_dt': '{{a-gajdabura}}'}
    )


    def func_get_info_from_greenplum(execution_dt):
        weekday_id = func_get_weekday_id(execution_dt)
        logging.info("Trying to get Info from GP: "+str(weekday_id)+' {{a-gajdabura}}')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        sql_text = 'SELECT heading FROM articles WHERE id = '+str(weekday_id+1)
        cursor.execute(sql_text)  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение

        return query_res

    get_Info_from_GreenPlum_to_XCom_task = PythonOperator(
        task_id="get_Info_from_GreenPlum_task",
        python_callable=func_get_info_from_greenplum,
        op_kwargs={'execution_dt': '{{a-gajdabura}}'}
    )

    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success'
    )

    start_task >> no_sunday_task >> get_Info_from_GreenPlum_to_XCom_task >> end_task