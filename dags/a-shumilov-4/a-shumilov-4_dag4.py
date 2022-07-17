"""даг для XCom"""

from airflow import DAG, macros
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from pprint import  pprint
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(4),
    'owner': 'a-shumilov-4',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': 10,
    'priority_weight': 2,
    'end_date': datetime(2022, 2, 7)

}
SQL_SELECT = 'select heading FROM articles WHERE id = {}'
'select heading FROM articles WHERE id = {{ macros.ds_format(a-gajdabura, "%Y-%m-%d", "%w") }}'

dag = DAG("a-shumilov-4_dag4_XCom",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-shumilov-4']
          )

start = DummyOperator(task_id='start',
                      dag=dag)


def select_week_day_func(ds, **kwargs):
    pg_hook = PostgresHook('conn_greenplum')
    conn = pg_hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(SQL_SELECT.format(macros.ds_format(ds, "%Y-%m-%d", "%w")))
    query_res = cursor.fetchall()
    logging.info(query_res)

    kwargs['ti'].xcom_push(value=query_res, key='week')
    conn.close()
    return query_res

select_week_day = PythonOperator(
    task_id='select_week_day',
    python_callable=select_week_day_func,
    dag=dag
)

def print_out_imp_func(**kwargs):
    out = kwargs['templates_dict']['implicit']
    logging.info(out)

def print_out_exp_func(**kwargs):
    out = kwargs['ti'].xcom_pull(task_ids='select_week_day', key='week')
    logging.info(out)

print_out_imp = PythonOperator(
    task_id = 'print_out_imp',
    python_callable = print_out_imp_func,
    templates_dict = {'implicit': '{{ ti.xcom_pull(task_ids="select_week_day") }}'},
    provide_context = True,
    dag = dag
)

print_out_exp = PythonOperator(
    task_id = 'print_out_exp',
    python_callable = print_out_exp_func,
    dag = dag
)

end = BashOperator(
        task_id = 'end',
        bash_command = 'echo "End!"',
        dag = dag
)

start >> select_week_day >> [print_out_imp, print_out_exp] >> end