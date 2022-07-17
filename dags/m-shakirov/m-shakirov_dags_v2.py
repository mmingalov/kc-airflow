# Доработка с учётом Greenplum
# Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)
# Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
# Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо настройте его самостоятельно в вашем личном Airflow. Параметры соединения:
# Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
# Выводить результат работы в любом виде: в логах либо в XCom'е
# Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta, date
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'm-shakirov',
    'poke_interval': 600,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    }


with DAG(
    dag_id='m-shakirov_dags_v2',
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=3,
    tags=['m-shakirov'],
    catchup=True
) as dag:

    #start = DummyOperator(task_id='start')

    def load_heading_to_xcom_func(exec_date=None, **kwargs):
        
        def get_data_gp(cursor, exec_date=None):
            # Get weekday
            if not exec_date:
                execution_date = kwargs['execution_date']
            else:
                execution_date = exec_date
            week_day = execution_date.weekday() + 1
            query = f'SELECT heading FROM articles WHERE id = {week_day}'
            cursor.execute(query)
            query_res = cursor.fetchall()
            # Logging result
            logging.info('--------')
            logging.info(f'Weekday: {week_day} of {execution_date}')
            logging.info(f'Return value: {query_res}')
            logging.info('--------')
            cursor.close()


        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor('named_cursor_name')
        query_res = get_data_gp(cursor)

        kwargs['ti'].xcom_push(value=query_res, key='heading')

    greenplum_conn = PythonOperator(
        task_id='greenplum_conn',
        python_callable=load_heading_to_xcom_func,
        provide_context=True
    )

    
    greenplum_conn