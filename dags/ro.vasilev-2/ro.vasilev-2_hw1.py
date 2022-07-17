from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'ro.vasilev-2',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='ro.vasilev-hw',
    schedule_interval='0 10 * * 1-6',
    default_args=DEFAULT_ARGS,
    tags=['dreatrio']
) as dag:

    def get_data(**context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор

        day_of_week = datetime.today().weekday() + 1

        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(day_of_week))  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение

        context['ti'].xcom_push(value=query_res, key='data')


    start = DummyOperator(task_id='start')

    get_data = PythonOperator(
        task_id='get_data',
        provide_context=True,
        python_callable=get_data,
    )

    end = DummyOperator(task_id='end')

start >> get_data >> end

