"""
Забираем из таблицы articles значение поля heading из строки с id, равным дню недели ds 
"""
import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import get_current_context


DEFAULT_ARGS = {
    'start_date':  datetime(2022, 3, 1),
    'end_date':  datetime(2022, 3, 14),
    'owner': 'a.gracheva',
    'poke_interval': 600,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
   
}

@dag(
    "a.gracheva-2",
    schedule_interval= '10 10 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup = True,
    tags=['a.gracheva']
) 

def get_first_line_from_gp():

    @task
    def get_today():
        context = get_current_context()
        ds = context["ds"]
        weekday = datetime.strptime(ds, '%Y-%m-%d').weekday()+1
        return weekday
    
    @task
    def get_first_line(today):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {today}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        return query_res

    @task(do_xcom_push = True)
    def print_line(total):
        logging.info(total)
    

    task1 = get_today()
    task2 = get_first_line(task1)
    task3 = print_line(task2)

a_gracheva_dag = get_first_line_from_gp()