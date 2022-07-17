from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta


DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'e.gabriel',
    'poke_interval': 600
}


dag = DAG("eugen_simple_dag_v2",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          )
wait_until_6am = TimeDeltaSensor(
    task_id='wait_until_6am',
    delta=timedelta(seconds=6 * 60 * 60),
    dag=dag
) 