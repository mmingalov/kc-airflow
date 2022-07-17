"""Этот Даг
создает таблицу, если есть вчерашние данные он их удаляет
три локации сериала "Рик и Морти" с наибольшим количеством резидентов,
записывает значения соответствующих полей этих трёх локаций в таблицу.
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
    'owner': 'o.fedchenko-2',
    'retries': 2,
    'start_date': datetime(2021, 11, 22),
    'end_date': datetime(2021, 12, 31)
}

dag = DAG(
    'o.fedchenko-2_ramdag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=['karpov']
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='conn_greenplum_write',
    sql="""CREATE TABLE IF NOT EXISTS o_fedchenko_2_ram_location (
    Surrogate_key SERIAL PRIMARY KEY,
               id INT NOT NULL,
             name VARCHAR NOT NULL,
             type VARCHAR NOT NULL,
        dimension VARCHAR NOT NULL,
     resident_cnt INT NOT NULL);                  
        """,
    dag=dag
)

def delete_data():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM o_fedchenko_2_ram_location;')
    conn.commit()
    cursor.close()
    conn.close()


def page_count(**kwargs):  # общее количества страниц  из api
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


def top_three_loc(**kwargs):  # топ 3 локаций по количеству персонажей
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


def insert(**kwargs):  # найденные локаций вставляются в таблицу
    top_three_var = kwargs['ti'].xcom_pull(task_ids='top_three_loc', key='top_three')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = 'INSERT INTO o_fedchenko_2_ram_location (id, name, type, dimension, resident_cnt) VALUES %s;'
    for i in range(3):
        cursor.execute(query, (top_three_var[i],))
        conn.commit()
    cursor.close()
    conn.close()


start = DummyOperator(task_id='start', dag=dag)


delete_data = PythonOperator(
    task_id='delete_data',
    python_callable=delete_data,
    dag=dag
)

count_page = PythonOperator(
    task_id='count_page',
    python_callable=page_count,
    dag=dag
)

top_three_loc = PythonOperator(
    task_id='top_three_loc',
    python_callable=top_three_loc,
    dag=dag
)

insert_into_table = PythonOperator(
    task_id='insert_into_table',
    python_callable=insert,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> create_table >> delete_data >> count_page >> top_three_loc >> insert_into_table >> end