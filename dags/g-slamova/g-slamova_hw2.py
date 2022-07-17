import csv
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'g-slamova',
    'poke_interval': 600
}

with DAG("g-slamova_hw2",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['g-slamova']
) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag
    )


    def get_page_count(api_url):
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in loading page count')


    def get_max_residents_on_page(result_json):
        ram_info = []
        ram_info_top3 = []
        for one_char in result_json:
            ram_info.insert(one_char.get('id') - 1, [one_char.get('id'), str(one_char.get('name')),
                                                     str(one_char.get('type')), str(one_char.get('dimension')),
                                                     len(one_char.get('residents'))])
        ram_info_sorted = sorted(ram_info, key=lambda x: x[4], reverse=True)
        for i in range(3):
            ram_info_top3.insert(i, ram_info_sorted[i])
        return ram_info_top3


    def load_ram_func():
        ram_info_total_top = []
        ram_info_total_top3 = []
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                ram_info_top_per_page = get_max_residents_on_page(r.json().get('results'))
                logging.info(f'top per page = {ram_info_top_per_page}')
                for i in range(3):
                    ram_info_total_top.insert(i, ram_info_top_per_page[i])
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        ram_info_total_top_sorted = sorted(ram_info_total_top, key=lambda x: x[4], reverse=True)
        for i in range(3):
            ram_info_total_top3.insert(i, ram_info_total_top_sorted[i])
        logging.info(f'top3 from total = {ram_info_total_top3}')

        with open("/tmp/g_slamova_ram.csv", "w+") as my_csv:
            mycsv = csv.writer(my_csv, delimiter=',')
            mycsv.writerows(ram_info_total_top3)


    load_ram = PythonOperator(
        task_id='load_ram',
        python_callable=load_ram_func,
        dag=dag
    )


    def load_csv_to_greenplum_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.run('truncate table g_slamova_ram_location', False)
        pg_hook.copy_expert("COPY g_slamova_ram_location FROM STDIN DELIMITER ','", '/tmp/g_slamova_ram.csv')


    load_csv_to_greenplum = PythonOperator(
        task_id='load_csv_to_greenplum',
        python_callable=load_csv_to_greenplum_func,
        dag=dag
    )

start >> load_ram >> load_csv_to_greenplum