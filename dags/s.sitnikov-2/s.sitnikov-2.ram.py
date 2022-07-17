"""Даг, который создает таблицу, удаляет вчерашние данные, находит три топовых локации по кол-ву персонажей,
   вставляет эти данные в таблицу.
   Состоит из 4 - PythonOperator; 2 - DummyOperator; 1 - PostgresOperator; 1 - PostgresHook
"""

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
import logging
import psycopg2
import requests

DEFAULT_ARGS = {
    'owner': 's.sitnikov-2',
    'retries': 2,
    'start_date': datetime(2021, 11, 20),
    'end_date': datetime(2025, 11, 25)
}

dag = DAG(
    's.sitnikov-2.ram',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=['lesson 5', 'karpov', 's.sitnikov-2', ]
)


def delete_data_func():  # удаление вчерашних данных
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM s_sitnikov_2_ram_location;')
    conn.commit()
    cursor.close()
    conn.close()


def page_count_func(**kwargs):  # получение общего кол-ва страниц api
    api_url = 'https://rickandmortyapi.com/api/location'
    r = requests.get(api_url)
    if r.status_code == 200:
        logging.info('SUCCESS')
        page_count = r.json().get('info').get('pages')
        logging.info(f'page_count = {page_count}')
        kwargs['ti'].xcom_push(value=page_count, key='page_count')
    else:
        logging.warning(f'HTTP STATUS {r.status_code}')
        raise AirflowException('Error in load page count')


def top_three_loc_func(**kwargs):  # нахождение топ три локаций по кол-ву персонажей
    page_count = kwargs['ti'].xcom_pull(task_ids='count_page', key='page_count')
    result = []
    ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
    for page in range(1, page_count + 1):
        r = requests.get(ram_char_url.format(pg=str(page)))
        result_json = r.json().get('results')
        if r.status_code == 200:
            logging.info(f'PAGE {page}')
            for c in result_json:
                result.append((c.get('id'), c.get('name'), c.get('type'), c.get('dimension'), len(c.get('residents'))))
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load from Rick&Morty API')
    sort_result = sorted(result, key=lambda x: x[4], reverse=True)
    top_three_var = sort_result[:3]
    logging.info(top_three_var)
    kwargs['ti'].xcom_push(value=top_three_var, key='top_three')


def insert_func(**kwargs):  # вставка найденных локаций в таблицу
    top_three_var = kwargs['ti'].xcom_pull(task_ids='top_three_loc', key='top_three')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = 'INSERT INTO s_sitnikov_2_ram_location (id, name, type, dimension, resident_cnt) VALUES %s;'
    for i in range(3):
        cursor.execute(query, (top_three_var[i],))
        conn.commit()
    cursor.close()
    conn.close()


start = DummyOperator(task_id='start', dag=dag)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='conn_greenplum_write',
    sql="""CREATE TABLE IF NOT EXISTS s_sitnikov_2_ram_location (
    Surrogate_key SERIAL PRIMARY KEY,
               id INT NOT NULL,
             name VARCHAR NOT NULL,
             type VARCHAR NOT NULL,
        dimension VARCHAR NOT NULL,
     resident_cnt INT NOT NULL);                  
        """,
    dag=dag
)

delete_data = PythonOperator(
    task_id='delete_data',
    python_callable=delete_data_func,
    dag=dag
)

count_page = PythonOperator(
    task_id='count_page',
    python_callable=page_count_func,
    dag=dag
)

top_three_loc = PythonOperator(
    task_id='top_three_loc',
    python_callable=top_three_loc_func,
    dag=dag
)

insert_into_table = PythonOperator(
    task_id='insert_into_table',
    python_callable=insert_func,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> create_table >> delete_data >> count_page >> top_three_loc >> insert_into_table >> end