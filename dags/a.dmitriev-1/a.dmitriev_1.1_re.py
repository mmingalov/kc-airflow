from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG('a.dmitriev_dag_2_re',
          start_date=days_ago(10),
          schedule_interval='0 0 * * 1-6',  # works on Mon-Sat, not Sun
          )


bash_operation1 = BashOperator(
    bash_command='echo "task_start"',
    dag=dag,
    task_id='logging_start'
)

dummy_operation1 = DummyOperator(
    task_id='pause1',
    trigger_rule='all_success',
    dag=dag
)


def get_number_of_week_func():
    number_of_week = datetime.datetime.now().weekday() + 1  # Monday = 1; Sunday = 7
    return number_of_week


execution_date = PythonOperator(
    task_id='execution_date',
    python_callable=get_number_of_week_func,
    dag=dag
)


def go_to_greenplum_func(**kwargs):
    ti = kwargs['ti']
    exec_date = ti.xcom_pull(task_ids='execution_date')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("my_cursor")  # и именованный (необязательно) курсор
    cursor.execute("SELECT heading FROM articles WHERE id = {}".format(exec_date))   # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    result = ti.xcom_push(value=query_res, key='article')


go_to_greenplum = PythonOperator(
    task_id='get_data',
    dag=dag,
    python_callable=go_to_greenplum_func
)

bash_operation4 = BashOperator(
    bash_command='echo "all OK"',
    dag=dag,
    task_id='bash_echo_2'
)

end_operation = DummyOperator(
    task_id='end',
    dag=dag
)


bash_operation1 >> dummy_operation1 >> execution_date >> go_to_greenplum >> bash_operation4 >> end_operation