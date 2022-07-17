"""
Первый ДАГ
Недельный
"""
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.timezone import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 's-chernoguzov-7',
    'poke_interval': 600
}

with DAG("greenplum_chernoguzov",
         schedule_interval='0 6 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-chernoguzov-7']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    def greenplum_pull_func(id, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("SC_cursor")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {id};')  # исполняем sql
        return cursor.fetchall()  # полный результат


    greenplum_pull = PythonOperator(
        task_id='greenplum_pull',
        python_callable=greenplum_pull_func,
        op_args=['{{ dag_run.logical_date.weekday()}} +1', ],
        do_xcom_push=True,
        dag=dag
    )

    dummy >> greenplum_pull
