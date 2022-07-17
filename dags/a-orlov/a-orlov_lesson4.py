from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'Lexx Orlov',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14)
}

dag = DAG('the_dag_v2',
          description='DAG from lesson 4',
          default_args=DEFAULT_ARGS,
          schedule_interval='@daily',
          max_active_runs=1,
          tags=['lexxo']
          )

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

def get_weekday_func(**kwargs):
    weekday = kwargs['execution_date'].weekday() + 1
    print('Current week day:', weekday)
    kwargs['ti'].xcom_push(key='weekday_num', value=weekday)

get_weekday = PythonOperator(
    task_id='get_weekday',
    python_callable=get_weekday_func,
    provide_context=True,
    dag=dag
)


def is_not_sunday(**kwargs):
    weekday = kwargs['ti'].xcom_pull(task_ids='get_weekday', key='weekday_num')
    return weekday != 7

sunday_off = ShortCircuitOperator(
    task_id='sunday_is_off',
    python_callable=is_not_sunday,
    dag=dag
)


def py_conn_greenplum(**kwargs):
    weekday = kwargs['ti'].xcom_pull(task_ids='get_weekday', key='weekday_num')

    pg_hook = PostgresHook('conn_greenplum')
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute('SELECT heading FROM articles WHERE id = {weekday}'.format(weekday=weekday))  # исполняем sql
    query_res = cursor.fetchall()[0]  # полный результат
    # one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    kwargs['ti'].xcom_push(key='result', value=query_res)

connect_greenplum = PythonOperator(task_id='get_data_from_greenplum',
                         python_callable=py_conn_greenplum,
                         dag=dag)


start >> get_weekday >> sunday_off >> connect_greenplum >> end


dag.doc_md = __doc__
sunday_off.doc_md = 'Воскресенье - выходной!'