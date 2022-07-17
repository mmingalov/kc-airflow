"""> Задание
Время работы с понедельника по субботу
Ходить в наш GreenPlum (используем соединение 'conn_greenplum')
Забирать из таблицы articles значение поля heading из строки с id,
равным дню недели execution_date (понедельник=1, вторник=2, ...)
Складывать получившееся значение в XCom
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import timedelta, datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'v-efimov-7',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG("v-efimov-7-3",
          schedule_interval='0 0 * * 1-6',#запуск с понедельника по субботу
          default_args=default_args,
          start_date=days_ago(3),
          max_active_runs=1,
          tags=['v-efimov-7']
          ) as dag:

    begin = DummyOperator(task_id='begin')

    def takin_from_db_func(**context):#функция извлечения данны из БД
        data_interval_start = context['data_interval_start']
        weekday = data_interval_start.weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')# инициализируем хук
        conn = pg_hook.get_conn() # берём из него соединение
        cur = conn.cursor() #создаём курсор
        qry = str(f'SELECT heading FROM articles WHERE id = {weekday}') #формируем запрос
        cur.execute(qry) #выполняем запрос
        rez = cur.fetchall()
        conn.close()
        logging.info(str(weekday) + " weekday execute")
        context['ti'].xcom_push(value=rez, key='article')

    takin_from_db = PythonOperator(
        task_id='takin_from_db',
        python_callable=takin_from_db_func,
        provide_context=True,
        dag = dag
        )

    end = DummyOperator(task_id='end')

    begin >> takin_from_db >> end