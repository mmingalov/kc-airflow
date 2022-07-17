"""
Даг обращается к Greenplum к таблице articles;
забирает данные за период 03.01-03.14;
выводит в консоль данные, где id=день_недели.
"""

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
import logging
from airflow import DAG

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'v-kosse',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'poke_interval': 60,
    'sla': timedelta(hours=1)
}

with DAG(
        "kosse_lesson_4",
        schedule_interval='0 10 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        catchup=True,
        tags=['v-kosse']
) as dag:
    def get_greenplum_data_func(exec_date):
        day_of_week = datetime.strptime(exec_date, '%Y-%m-%d').weekday() + 1
        logging.info(f'{exec_date} - date, {day_of_week} - day of week')

        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sql = f'''select heading 
                from articles
                where id = {day_of_week};
            '''
        cursor.execute(sql)
        result = cursor.fetchall()
        logging.info(result)

    get_greenplum_data = PythonOperator(
        task_id='get_greenplum_data',
        python_callable=get_greenplum_data_func,
        op_args=['{{ ds }}']
    )

get_greenplum_data
