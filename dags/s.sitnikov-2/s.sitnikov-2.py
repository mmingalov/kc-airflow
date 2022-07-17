"""
Это простой даг который: работает с понедельника по субботу, соединяется с GreenPlum, забирает из таблицы articles
значение поля heading из строки c id, равным дню недели, складывает получившееся значения в XCom
Даг состоит из:
3 - BashOperator,
2 - DummyOperator,
2 - PythonOperator,
1 - BranchPythonOperator,
1 - ShortCircuitOperator.
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
import logging
import psycopg2

DEFAULT_ARGS = {
    'owner': 's.sitnikov-2',
    'retries': 2,
    'start_date': days_ago(31),
    'trigger_rule': 'one_success'
}

dag = DAG(
    's.sitnikov-2.dag_1',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    tags=['lesson 4', 'karpov', 's.sitnikov-2']
)


def select_random_func():  # TASK select_random
    import random
    return random.choice(['task_1', 'task_2', 'task_3'])


def not_sunday_func(execution_dt):  # TASK work_monday_saturday
    exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
    return exec_day in [0, 1, 2, 3, 4, 5]


def load_heading_from_gp_func(execution_dt, **kwargs):  # TASK load_heading_from_gp
    exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT heading FROM articles WHERE id = (%s)", (exec_day,))
    logging.info('got it!')
    query_result = cursor.fetchone()
    kwargs['ti'].xcom_push(value=query_result, key='gp_heading')


def print_result_func(**kwargs):  # TASK pull_from_result_gp
    kwargs['ti'].xcom_pull(task_ids='load_heading_from_gp', key='gp_heading')


start = DummyOperator(task_id='start', dag=dag)

work_monday_saturday = ShortCircuitOperator(
    task_id='work_monday_saturday',
    python_callable=not_sunday_func,
    op_kwargs={'execution_dt': '{{ a-gajdabura }}'},
    dag=dag
)

select_random = BranchPythonOperator(
    task_id='select_random_task',
    python_callable=select_random_func,
    dag=dag
)

task_1 = BashOperator(
    task_id='task_1',
    bash_command='echo Hi',
    dag=dag
)

task_2 = BashOperator(
    task_id='task_2',
    bash_command='echo Hi there',
    dag=dag
)

task_3 = BashOperator(
    task_id='task_3',
    bash_command='echo Aloha',
    dag=dag
)

load_heading_from_gp = PythonOperator(
    task_id='load_heading_from_gp',
    python_callable=load_heading_from_gp_func,
    op_kwargs={'execution_dt': '{{ a-gajdabura }}'},
    provide_context=True,
    dag=dag
)

pull_from_result_gp = PythonOperator(
    task_id='pull_from_result_gp',
    python_callable=print_result_func,
    provide_context=True,
    dag=dag
)
end = DummyOperator(task_id='end', dag=dag)

start >> work_monday_saturday >> select_random >> [task_1, task_2,
                                                   task_3] >> load_heading_from_gp >> pull_from_result_gp >> end

dag.doc_md = __doc__

work_monday_saturday.doc_md = """Оператор ветвления, после которого таски работают с понедельника по субботу 
включительно. """
select_random.doc_md = """Оператор ветвления. Рандомно выбирает таску."""
task_1.doc_md = """Оператор. Выводит в лог приветствие."""
load_heading_from_gp.doc_md = """Забирает данные из gp и пушит в xcom."""
pull_from_result_gp.doc_md = """забирает параметры из xcom."""

