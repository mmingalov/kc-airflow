from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import logging



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a.kalinin-2'
}

dag = DAG("a.kalinin-2_first_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          )

dummy_op_start = DummyOperator(
    task_id='start_task',
    dag=dag,
  )

dummy_op_end = DummyOperator(
    task_id='end_task',
    dag=dag,
  )

echo_op = BashOperator(
    task_id='echo_op',
    bash_command='echo {{ a-gajdabura }}',
    dag=dag
)

def print_string():
    logging.info("It's a string")


python_task = PythonOperator(
    task_id='python_task_task',
    python_callable=print_string,
    dag=dag
)

dummy_op_start >> echo_op >> python_task >> dummy_op_end

dag.doc_md = __doc__

dummy_op_start.doc_md = """dummy start"""
dummy_op_enddoc_md = """dummy end"""
echo_op.doc_md = """writes execution date"""
python_task.doc_md = """Python function. Prints something'"""
