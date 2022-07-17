"""
Сергей Коноплев
Простейший даг, задание 4
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 's-konoplev-3',
    'poke_interval': 600
}

dag = DAG("sk_test_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-konoplev-3']
          )

wait_5sec = TimeDeltaSensor(
    task_id='wait_5sec',
    delta=timedelta(seconds=1*5),
    dag=dag
)

wait_10sec = TimeDeltaSensor(
    task_id='wait_10sec',
    delta=timedelta(seconds=1*10),
    dag=dag
)

echo_dt = BashOperator(
    task_id='echo_dt',
    bash_command='now="$(date)"; echo "$now";',
    dag=dag
)


def first_func():
    logging.info("write one string to log")


first_task = PythonOperator(
    task_id='first_task',
    python_callable=first_func,
    dag=dag
)


def second_func():
    logging.info("write another one string to log")


second_task = PythonOperator(
    task_id='second_task',
    python_callable=second_func,
    dag=dag
)

third_task = DummyOperator(
    task_id='third_task',
    dag=dag
)


end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)


"""
Проверил, можно ли запустить один таск дважды внутри одного DAG
Не совсем было понятно, что есть цикл (зацикливание).
НЕ СРАБОТАЛО, пробую повторно ожидание 10 сек. Верноятно, не имеет смысла.  
"""
wait_5sec >> echo_dt >> wait_10sec >> [first_task, second_task, third_task] >> end_task

dag.doc_md = __doc__

wait_5sec.doc_md = """Сенсор. Ждёт 5 секунд"""
wait_10sec.doc_md = """Сенсор. Ждёт 10 секунд"""
echo_dt.doc_md = """Пишет в лог execution_time"""
first_task.doc_md = """write one string to log'"""
second_task.doc_md = """write another one string to log'"""
third_task.doc_md = """ничего не делает'"""
end_task.doc_md = """Завершает DAG'"""
