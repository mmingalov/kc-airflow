# 5 модуль, 5 урок
# ДАГ выполняет следующие шаги:
# TBD


from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import datetime
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

import requests
from operator import itemgetter

from aromashkin_plugins.aromashkin_ram_top_tree_locations import aromashkin_ram_top_tree_locations_Operator

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'romashkin',
    'poke_interval': 600
}

dag = DAG("a.romashkin_ram_location_max_residents_count_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov', 'greenplum', 'XCom']
          )
def CreateTableGreenplumFunc():
    # 02. Подключаемся к Greenplum
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
    logging.info("CREATE TABLE") 
    pg_hook.run(
    """
    CREATE TABLE IF NOT EXISTS
    public.a_romashkin_2_ram_location(
        id integer PRIMARY KEY,
        name text,
        type text,
        dimension text,
        resident_cnt integer
        )
    """
    , False
    )
    logging.info("TRUNCATE TABLE") 
    pg_hook.run("TRUNCATE public.a_romashkin_2_ram_location", False)


CreateTableGreenplum = PythonOperator(
    task_id='CreateTableGreenplum',
    python_callable=CreateTableGreenplumFunc,
    dag=dag
)

start_sensor = TimeDeltaSensor(
    task_id='start_sensor',
    delta=timedelta(seconds=6*60*60),
    dag=dag
)

ram_top_tree_locations = aromashkin_ram_top_tree_locations_Operator(
   task_id='ram_top_tree_locations',
    dag=dag
)

start_sensor  >> CreateTableGreenplum >>  ram_top_tree_locations