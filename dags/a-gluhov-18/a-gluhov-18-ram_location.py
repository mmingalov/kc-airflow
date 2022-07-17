""" Забираем данные из rickandmortyapi.com,
    находим топ3 локации по числу резидентов
    и перемещаем их в таблицу в GreenPlum"""

import requests
from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
from a_gluhov_18_plugins.a_gluhov_18_operator import A_Gluhov_18_Ram_Top_Location



DEFAULT_ARGS = {'start_date': datetime(2022, 5,8),
    #'end_date': datetime(2022, 5, 11),
    'owner': 'a-gluhov',
    'poke_interval': 600,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval' : '00 12 * * *',
    'catchup':False
    }


api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'

with DAG('a-gluhov-18_top_locations',
         default_args = DEFAULT_ARGS,
         tags = ['gluhov'],
         ) as dag:

    start = DummyOperator(task_id = 'start', dag = dag)

    end = DummyOperator(task_id = 'end', dag = dag)

    top_3_location = A_Gluhov_18_Ram_Top_Location(task_id = 'top_3_location',
                                                  dag = dag)
    
    load_data_to_gp = PostgresOperator(task_id = 'load_data_to_gp',
                                       dag=dag,
                                       postgres_conn_id = 'conn_greenplum_write',
                                       sql = ['TRUNCATE TABLE a_gluhov_18_ram_location',
                                             'INSERT INTO public.a_gluhov_18_ram_location '
                                             'VALUES {{ ti.xcom_pull(task_ids = "top_3_location") }}']
                                             )

    start >> top_3_location >> load_data_to_gp >> end