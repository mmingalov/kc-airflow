"""
This is copy of first DAG
Yet simple but with connection to Greenplum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'a-tikhomirov-3',
    'poke_interval': 600
}

dag = DAG("a-tikhomirov-3-second-dag",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-tikhomirov-3']
          )

"""
Greenplum select
"""

def greenplum_get(day):

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("current_cursor")
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day}')
    db_result = cursor.fetchone()[0]
    # let's log this one
    logging.info(db_result)
    # and return result
    return db_result


"""
Now operators
"""

start_task = DummyOperator(task_id='start_task', dag=dag)

end_task = DummyOperator(task_id='end_task', dag=dag)

xcom_task = PythonOperator(
    task_id='xcom_greenplum_select',
    python_callable=greenplum_get,
    op_args=['{{ data_interval_start.weekday() }}', ],
    do_xcom_push=True,
    dag=dag
)

"""
Ok, all tasks together in DAG
"""

start_task >> xcom_task >> end_task