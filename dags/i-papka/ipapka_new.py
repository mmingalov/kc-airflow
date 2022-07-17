"""
Даг подсчета cnt с PythonOperator;
"""

from airflow import DAG
from airflow.utils.dates import days_ago


from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
#from ipapka_plugins.ipapka_ram_species_count_operator import IpapkaRamSpeciesCountOperator



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-papka',
    'poke_interval': 600
}

import requests
import logging

from airflow import AirflowException


def get_location_count(api_url: str) -> int:
    """
    Get count of locations in API
    :param api_url
    :return: locations count
    """
    r = requests.get(api_url)
    if r.status_code == 200:
        logging.info("SUCCESS")
        loc_count = r.json().get('info').get('count')
        logging.info(f'location_count = {loc_count}')

        return int(loc_count)
    else:
        logging.warning("HTTP STATUS {}".format(r.status_code))
        raise AirflowException('Error in load locations count')


def get_location_values(result_json: dict) -> list:
    """
    Get values from location
    :param result_json:
    :return: list_of_values
    """
    list_of_names = ['id', 'name', 'type', 'dimension', 'residents']
    list_of_values = []
    for name in list_of_names:
        if name == 'residents':
            list_of_values.append(len(result_json.get(name)))
        else:
            list_of_values.append(result_json.get(name))

    logging.info(f'Values from location = {list_of_values}')
    return list_of_values


def execute(**context):
    dct = {}
    url='https://rickandmortyapi.com/api/location/'
    for num in range(1, get_location_count(url) + 1):
        location_url = 'https://rickandmortyapi.com/api/location/{num}'
        r = requests.get(location_url.format(num=num))
        result_json = r.json()
        list_of_values = get_location_values(result_json)
        dct[list_of_values[0]] = list_of_values[1:]
    dct = {k: v for k, v in sorted(dct.items(), key=lambda x: x[1][3], reverse=True)[:3]}
    logging.info(dct)
    context['ti'].xcom_push(value=dct, key='result')



with DAG("ipapka_new",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-papka']
         ) as dag:

    start = DummyOperator(task_id='start')

    execute_task = PythonOperator(
        task_id='execute_task',
        python_callable=execute,
        provide_context=True,
        dag=dag
    )

    create_table_sql_query = """ 
                CREATE TABLE IF NOT EXISTS "i-papka_ram_location" (
                id serial4,
                name varchar,
                type varchar,
                dimension varchar,
                resident_cnt int4);
                TRUNCATE TABLE  "i-papka_ram_location";
            """

    create_table = PostgresOperator(
        sql=create_table_sql_query,
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        dag=dag
    )



    def insert_values_func(**context):
        dct = context['ti'].xcom_pull(key='result', task_ids='execute_task')
        logging.info(dct)
        values = [(k, v[0],v[1], v[2], v[3]) for k,v in dct.items()]
        logging.info(values)
        query = 'INSERT INTO "i-papka_ram_location"(id, name, type, dimension, resident_cnt) VALUES (%s,%s,%s,%s,%s);'
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        cursor.executemany(query, values)  # исполняем sql
        conn.commit()



    insert_values = PythonOperator(
        task_id='insert_values',
        python_callable=insert_values_func,
        provide_context=True
    )


    start  >> create_table >> execute_task >> insert_values