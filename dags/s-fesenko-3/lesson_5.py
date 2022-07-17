"""> Задание
Нужно доработать даг, который вы создали на прошлом занятии.

Он должен:
Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)+
Ходить в наш GreenPlum (используем соединение 'conn_greenplum'. Вариант решения — PythonOperator с PostgresHook внутри)
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели execution_date (понедельник=1, вторник=2, ...)
Складывать получившееся значение в XCom
Результат работы будет виден в интерфейсе с XCom.

pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
conn = pg_hook.get_conn()  # берём из него соединение
cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
cursor.execute('SELECT heading FROM articles WHERE id = 1')  # исполняем sql
query_res = cursor.fetchall()  # полный результат
one_string = cursor.fetchone()[0]  # если вернулось единственное значение"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import timedelta, datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 's-fesenko-3',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG("lesson_5",
          schedule_interval='0 0 * * 1-6',
          default_args=default_args,
          start_date=days_ago(5),
          max_active_runs=1,
          tags=['s-fesenko-3']
          ) as dag:

    begin = DummyOperator(task_id='begin')

    def takin_from_db_func(**context):
        data_interval_start = context['data_interval_start']
        weekday = data_interval_start.weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        qry = str(f'SELECT heading FROM articles WHERE id = {weekday}')
        cur.execute(qry)
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

    def log_ds_func(**kwargs):
        date = kwargs['templates_dict']['a-gajdabura']
        logging.info(date)

    log_ds = PythonOperator(
        task_id='log_ds',
        python_callable=log_ds_func,
        templates_dict= {'a-gajdabura':' {{ a-gajdabura }} '},
        dag = dag
    )

    end = DummyOperator(task_id='end')

    begin >> takin_from_db >> log_ds >> end

dag.doc_md = __doc__

begin.doc_md = """Начало DAG'а"""
current_date = """находим нужный ИД соответственно дню"""
takin_from_db.doc_md = """Выгрузка результата запроса GP за соответствующий день недели в xcom airflow"""
end.doc_md = """Конец DAG'а""" # y tre sasd
