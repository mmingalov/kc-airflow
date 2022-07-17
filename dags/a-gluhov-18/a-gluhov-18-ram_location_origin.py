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
from airflow.exceptions import AirflowException

DEFAULT_ARGS = {'start_date': datetime(2022, 5,9),
    #'end_date': datetime(2022, 5, 11),
    'owner': 'a-gluhov',
    'poke_interval': 600,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval' : '00 12 * * *',
    'catchup':False
    }

api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'

# функция для подсчета числа страниц с локациями
def get_page_count(url):
    r = requests.get(url.format(pg='1'))
    if r.status_code == '200':
        logging.info("SUCCESS")
        page_count = r.json().get('info').get('pages')
        return int(page_count)
    else:
        logging.warning("HTTP STATUS {}".format(r.status_code))
        raise AirflowException('Error in load page count')
    page_count = r.json().get('info').get('pages')
    return page_count

# функция для вычисления топ 3 локаций по кол-ву резидентов
def get_top_location():
    df = pd.DataFrame()                                                          #создаю пустой датафрейм
    for page in range(get_page_count(api_url)):                                  #идем по страницам location
        r = requests.get(api_url.format(pg=str(page + 1))).json().get('results') #получаем данные с result в виде списка
        for i in range(len(r)):                                                  #идем по длине списка
            df = df.append(r[i], ignore_index=True)                              #добавляем в датафрейм каждый элемент
    df['resident_cnt'] = df.residents.apply(lambda x: len(x))                  #добавляем столбец с кол-вом резидентов
    df = df.sort_values('resident_cnt', ascending=False).head(3)               #выводим топ 3
    df = df.drop(['created','url','residents'], 1)
    tuples = [tuple(x) for x in df.to_numpy()]
    return ','.join(map(str, tuples))

with DAG('a-gluhov-18_top_locations_origin',
         default_args = DEFAULT_ARGS,
         tags = ['gluhov'],
         ) as dag:

    start = DummyOperator(task_id = 'start', dag = dag)

    end = DummyOperator(task_id = 'end', dag = dag)

    top_3_location = PythonOperator(task_id = 'top_3_location',
                                    python_callable = get_top_location,
                                    provide_context=True,
                                    dag = dag)

    load_data_to_gp = PostgresOperator(task_id = 'load_data_to_gp',
                                       dag=dag,
                                       postgres_conn_id = 'conn_greenplum_write',
                                       sql = ['TRUNCATE TABLE a_gluhov_18_ram_location',
                                             'INSERT INTO public.a_gluhov_18_ram_location '
                                             'VALUES {{ ti.xcom_pull(task_ids = "top_3_location") }}']
                                             )

    start >> top_3_location >> load_data_to_gp >> end