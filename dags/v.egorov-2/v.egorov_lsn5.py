#!coding: utf-8
import requests
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging

from airflow import DAG

def fifth_element(tup):
    return tup[4]

def get_and_load(top):
    response = requests.get('https://rickandmortyapi.com/api/location/')
    arr = []
    for p in range(response.json().get('info').get('pages')):
        resp2 = requests.get('https://rickandmortyapi.com/api/location/?page={page}'.format(page=p + 1)).json().get(
            'results')
        for pe in resp2:
            arr.append(
                (pe.get('id'), pe.get('name'), pe.get('type'), pe.get('dimension'), len(pe.get('residents', []))))
    arr.sort(key=fifth_element, reverse=True)

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        '''create table if not exists "v.egorov-2_ram_location" (id int, name text, type text, dimension text, resident_cnt int);''')
    conn.commit()
    for i in range(top):
        cursor.execute('''insert into "v.egorov-2_ram_location" values {v};'''.format(v=arr[i]))
    conn.commit()
    conn.close()

dag = DAG(
    'v.egorov_lsn5',
    description='Rick_n_Morty_top3_crowded_spots',
    schedule_interval='@once',
    start_date=datetime(2021, 11, 24),
    max_active_runs=1,
    catchup=False,
    tags=['egorov']
)

single_main_task = PythonOperator(task_id='get_and_load', python_callable=get_and_load, op_kwargs={'top': 3}, dag=dag)




