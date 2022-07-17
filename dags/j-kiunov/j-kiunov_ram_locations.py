import requests
from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
from airflow.exceptions import AirflowException

DEFAULT_ARGS = {'start_date': datetime(2022, 5, 10),
    #'end_date': datetime(2022, 5, 12),
    'owner': 'j-kiunov',
    'poke_interval': 600,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval' : '00 12 * * *',
    'catchup':False
    }

api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'

def get_page_count(url):
    r = requests.get(url.format(pg='1'))
    page_count = r.json().get('info').get('pages')
    return page_count

def get_top_location():
    r = []
    for page in range(get_page_count(api_url)):
        r += requests.get(api_url.format(pg='page+1')).json().get('results') #проходим по страницам и записываем в r данные
    df = pd.DataFrame(r) #преобразуем данные в вид датафрейм
    df['resident_cnt'] = df.residents.apply(lambda x: len(x))  #Добавляем столбец с количеством резидентов
    df = df.sort_values('resident_cnt', ascending=False).head(3)               #выводим топ 3
    df = df.drop(['created','url','residents'], 1)
    return df

with DAG(
    'j-kiunov_ram_location',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['kiunov']
) as dag:
    start = DummyOperator(task_id = 'start', dag = dag)

    end = DummyOperator(task_id = 'end', dag = dag)

    top_3_location = PythonOperator(task_id = 'top_3_location',
                                    python_callable = get_top_location,
                                    provide_context=True,
                                    dag = dag)

#
    load_data_to_gp = PostgresOperator(task_id = 'load_data_to_gp',
                                       dag=dag,
                                       postgres_conn_id = 'conn_greenplum_write',
                                       sql = ['TRUNCATE TABLE j_kiunov_ram_location',
                                             'INSERT INTO public.j_kiunov_ram_location',
                                             ' VALUES {{ ti.xcom_pull(task_ids = "top_3_location") }}']
                                             )

    start >> top_3_location >> load_data_to_gp >> end
