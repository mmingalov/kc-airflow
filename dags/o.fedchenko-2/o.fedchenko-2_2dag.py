"""
Это второй даг.
Он должен:
Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)
Ходить в наш GreenPlum (импользуем соединение 'conn_greenplum'. Вариант решения — PythonOperator с PostgresHook внутри)
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели execution_date (понедельник=1, вторник=2, ...)
Складывать получившееся значение в XCom
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'o.fedchenko-2'
}

dag = DAG("o.fedchenko-2_2dag",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          )

dummy_op_start = DummyOperator(
    task_id='start_task',
    dag=dag,
  )

dummy_op_end = DummyOperator(
    task_id='end_task',
    dag=dag,
  )


def greenpulm(**context):
    execution_date = context['execution_date']
    weekday = execution_date.weekday() + 1
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday))
    query_res = cursor.fetchone()[0]
    logging.info("Got it")
    context['ti'].xcom_push(value=query_res, key='article')

greenplum_task = PythonOperator(
    task_id='greenplum_task',
    provide_context = True,
    python_callable=greenpulm,
    dag=dag
)



dummy_op_start >> greenplum_task >> dummy_op_end

dag.doc_md = __doc__

dummy_op_start.doc_md = """dummy start"""
dummy_op_enddoc_md = """dummy end"""
