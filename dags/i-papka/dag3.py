
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from random import randint

dag = DAG(dag_id='i-papka-dag',
          schedule_interval=timedelta(days=1), 
          start_date=days_ago(1))


# Функция которая всегда верна
def success():
  pass
# Функция вычислений;
def calc():
  n= randint(11,19)//10
  k=8
  res = k * n
  print(f'Result: {res}')
  return f'Result: {res}'

# Функция которая скипает задачу
def skip():
  raise AirflowSkipException

# Функция которая падает с ошибкой
def failed():
  raise AirflowFailException

task_0 = PythonOperator(
  task_id='task_0',
  python_callable=calc,
  dag=dag
)

task_1 = PythonOperator(
  task_id='task_1',
  python_callable=skip,
  dag=dag
)

task_2 = PythonOperator(
  task_id='task_2',
  python_callable=failed,
  dag=dag
)

task_3 = DummyOperator(
  task_id='task_3',
  trigger_rule='one_success'
)

task_4 = PythonOperator(
  task_id='task_4',
  python_callable=success,
  dag=dag
)

task_5 = PythonOperator(
  task_id='task_5',
  python_callable=failed,
  dag=dag
)

task_6 = BashOperator(
  task_id='task_6',
  bash_command='echo "Hello, Dina"',
  dag=dag
)

task_7 = PythonOperator(
  task_id='task_7',
  python_callable=lambda: print("Success"),
  trigger_rule='one_success',
  dag=dag
)

[task_0, task_1, task_2] >> task_3 >> [task_4, task_5,task_6] >> task_7
