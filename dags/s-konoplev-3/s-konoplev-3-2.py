"""
Сергей Коноплев
Даг + greenplum, задание 5
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 's-konoplev-3',
    'poke_interval': 600
}

dag = DAG("sk_test_dag_gp",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-konoplev-3']
          )

def first_func_get_data_from_db(day):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT heading FROM articles WHERE id = ' + day)
    query_res = cursor.fetchall()
    cursor.close()
    #logging.info(query_res)
    if len(query_res) == 1:
        result = query_res[0]
    else:
        result = query_res
    logging.info(result)
    return result



start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)


xcom_push = PythonOperator(
    task_id='xcom_push',
    python_callable=first_func_get_data_from_db,
    op_args=['{{ data_interval_start.weekday() }}', ],
    do_xcom_push=True,
    dag=dag
)

start_task >> xcom_push >> end_task

dag.doc_md = __doc__

xcom_push.doc_md = """Выгружает данные Gp -> Xcom'"""
end_task.doc_md = """Начинает DAG'"""
end_task.doc_md = """Завершает DAG'"""
