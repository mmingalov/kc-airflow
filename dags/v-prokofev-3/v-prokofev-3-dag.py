
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
from airflow.utils.timezone import datetime
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'karpov',
    'queue': 'karpov_queue',
    'pool': 'user_pool',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 10,
    'start_date': datetime(2021, 1, 1),
    'end_date': datetime(2025, 1, 1),
    'sla': timedelta(hours=2),
    'execution_timeout': timedelta(seconds=300),
 #   'on_failure_callback': some_function,
 #   'on_success_callback': some_other_function,
 #   'on_retry_callback': another_function,
 #   'sla_miss_callback': yet_another_function,
    'trigger_rule':  'all_success'
}
with DAG("v-prokofev-3-dag0",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          tags=['prokofev']
          ) as dag:

    def gp():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute('SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        return query_res


    end = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )
    implicit_push = PythonOperator(
        task_id='implicit_push',
        python_callable=gp
    )
    implicit_push >> end
