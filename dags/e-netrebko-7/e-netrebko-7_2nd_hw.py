import logging
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

my_id = 'e-netrebko-7'

default_args = {
    'owner': my_id,
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
}


def load_from_gp(**kwargs) -> None:
    weekday = datetime.strptime(str(kwargs['tomorrow_ds']), "%Y-%m-%d").isoweekday()
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
    one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(one_string)


with DAG(
    dag_id=f'{my_id}_2nd_hw',
    default_args=default_args,
    schedule_interval='0 0 * * 1-6',
) as dag:

    start_flow = DummyOperator(task_id='start_flow')
    end_flow = DummyOperator(task_id='end_flow', trigger_rule='all_done')

    load_from_gp = PythonOperator(
        task_id='load_from_gp',
        python_callable=load_from_gp,
        provide_context=True,
    )

    start_flow >> load_from_gp >> end_flow
