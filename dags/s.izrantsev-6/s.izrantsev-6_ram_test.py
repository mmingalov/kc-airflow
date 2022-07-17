"""
Гутен даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'Izrantsev',
    'poke_interval': 600
}

with DAG("izrantsev_dag_ram_locations_simple",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['izrantsev', 'Rick&Morty', 'ram']
         ) as dag:
    def get_locations_pages(api_url):

        r = requests.get(api_url)

        if r.status_code == 200:
            page_count = r.json().get('info').get('pages')
            logging.info("===============================================")
            logging.info("Connection success!")
            logging.info(f"Pages with locations Rick&Morty: {page_count}")
            logging.info("===============================================\n")
            return page_count
        else:
            logging.info("===============================================\n")
            logging.info(f"HTTP status {r.status_code}")
            logging.info("===============================================\n")
            raise AirflowException('Error in load page')


    def get_locations_tuples(locations_results):

        locations_tuples = list()

        for loc in locations_results:
            loc_id = loc.get('id')
            loc_name = loc.get('name')
            loc_type = loc.get('type')
            loc_dimension = loc.get('dimension')
            loc_resident_cnt = len(loc.get('residents'))

            locations_tuple = (loc_id, loc_name, loc_type, loc_dimension, loc_resident_cnt)
            locations_tuples.append(locations_tuple)

            output = 'id: {};name: {};type: {};dimension: {};resident_cnt: {}'

            logging.info("===============================================")
            logging.info(output.format(*locations_tuple))
            # logging.info("===============================================\n")

        return locations_tuples


    def three_most_char_locations():

        url = 'https://rickandmortyapi.com/api/location?page={pg}'
        locations = list()
        pages = get_locations_pages(url.format(pg='1'))
        for page in range(pages):
            r = requests.get(url.format(pg=str(page + 1)))

            if r.status_code == 200:
                logging.info("======================================================================================")
                logging.info(f'PAGE {page + 1}')
                logging.info("======================================================================================\n")
                results = r.json().get('results')
                locations += get_locations_tuples(results)
            else:
                logging.info("======================================================================================")
                logging.info(f"HTTP status {r.status_code}")
                logging.info("======================================================================================\n")
                raise AirflowException('Error in load page')

        final = sorted(locations, key=lambda i: i[-1], reverse=True)[:3]
        return ','.join(map(str, final))


    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    three_most_char_locations = PythonOperator(
        task_id='three_most_char_locations',
        python_callable=three_most_char_locations
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=['''
            create table if not exists s_izrantsev_6_ram_locations(
            id int
            ,name varchar
            ,type varchar 
            ,dimension varchar 
            ,resident_cnt int
            )
            distributed by (id);
            ''']
    )

    load_data = PostgresOperator(
        task_id='load_data_to_greenplum',
        postgres_conn_id='conn_greenplum_write',
        sql=['truncate s_izrantsev_6_ram_locations;',
             '''insert into s_izrantsev_6_ram_locations 
            values {{ ti.xcom_pull(task_ids='three_most_char_locations') }}
        ''']
    )


    def print_pass():
        conn = BaseHook.get_connection('conn_greenplum_write')

        logging.info('--------------------------------------------------------\n')
        logging.info('**** Details of conn_greenplum_write ****')
        logging.info(BaseHook.get_connection('conn_greenplum_write').password)
        logging.info('--------------------------------------------------------\n')
        logging.info('**** Details of dina_ram ****')
        logging.info(BaseHook.get_connection('dina_ram').password)
        logging.info('--------------------------------------------------------\n')
        logging.info('**** Details of conn_greenplum ****')
        logging.info(BaseHook.get_connection('conn_greenplum').password)
        logging.info('--------------------------------------------------------\n')
        logging.info('**** Another try getting password conn_greenplum_write ****')
        logging.info(conn.password)
        logging.info('--------------------------------------------------------\n')


    get_pass = PythonOperator(
        task_id='get_pass',
        python_callable=print_pass,
    )


    def gp_query(**kwargs):
        hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT * FROM s_izrantsev_6_ram_locations')
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(value=query_res, key='result')
        logging.info('--------------------------------------------------------\n')
        logging.info('**** Result of query is: {} ****'.format(query_res))
        logging.info('--------------------------------------------------------\n')


    gp_query = PythonOperator(
        task_id='gp_query',
        python_callable=gp_query,
    )

start >> three_most_char_locations >> create_table >> load_data >> gp_query >> get_pass >> end
