"""
В процессе стремления к выполнению задания, был получен сей код,
нагло собранный из трех источников. Выстраданный не только мною,
но и людьми добрыми.. Спасибо! =)
"""


import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow import DAG


### начало дага и его описание
with DAG(dag_id='rpuropuu_dag_002',
         default_args={'owner': 'rpuropuu',
                       'retries': 5,
                       'poke_interval': 300
                       },
         schedule_interval="0 0 * * 1-6",
         max_active_runs=1,
         start_date=days_ago(1),
         tags=['excercise_002']
    ) as dag:


    # функция получения из питона сегодняшнего деня недели и его коррекция, исходя из задания
    def get_day_num():
        day_num = datetime.datetime.today().weekday() + 1
        return day_num


    # функция подключения к conn_greenplum, описанное в аирфлоу
    def connect_to(**kwargs):
        ti = kwargs['ti']
        day_num = ti.xcom_pull(task_ids='getting_day_num')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cu = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cu.execute(f'SELECT heading FROM articles WHERE id = {day_num};')  # исполняем sql
        query_res = cu.fetchall()  # полный результат
        # one_string = cu.fetchone()[0]  # с этой строкой не хочет работать
        query_result = ti.xcom_push(value=query_res, key='article')


    # описание таски, инициализирующей получение дня недели
    getting_day_num = PythonOperator(
        task_id='getting_day_num',
        python_callable=get_day_num
    )

    # описание таски, инициализирующей извлечение данных исходя из дня недели
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=connect_to,
    )

    # пайплайн выполнения тасок
    getting_day_num >> extract_data

### конец дага
