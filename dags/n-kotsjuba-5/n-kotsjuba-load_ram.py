"""
Загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests
import json

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from n_kotsjuba_5_plugins.n_kotsjuba_ram_resident_count_operator import KotsjubaRamResidentCountOperator
#from dina_plugins.dina_ram_species_count_operator import DinaRamSpeciesCountOperator
#from dina_plugins.dina_ram_dead_or_alive_operator import DinaRamDeadOrAliveCountOperator
#from n_kotsyuba_5_plugins.n_kotsjuba_random_sensor import KotsjubaRandomSensor


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'n-kotsjuba-5',
    'poke_interval': 600
}


with DAG("n-kotsjuba-load_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-kotsjuba-5']
         ) as dag:

    start = DummyOperator(task_id='start')

    #random_wait = KotsjubaRandomSensor(task_id='random_wait', mode='reschedule')

    def get_page_count(api_url):
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def load_ram_func(**kwargs):
        locations = []
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                json_answer_text = json.loads(r.text)
                res = json_answer_text["results"]
                for item in res:
                    locations.append({
                        "id": item["id"],
                        "name": item["name"],
                        "type": item["type"],
                        "dimension": item["dimension"],
                        "resident_cnt": len(item["residents"])

                    })
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        locations_sorted = sorted(locations, key=lambda x: x["resident_cnt"], reverse=True)[:3]
        locations_str = ""
        for item in locations_sorted:
            locations_str += "(" + str(item["id"]) + ",'" + item["name"] + "'" + ",'" + item["type"] + "'" + ",'" + \
                             item["dimension"] + "'" + "," + str(item["resident_cnt"]) + "),"
        locations_str = locations_str[:-1]
        kwargs['ti'].xcom_push(key='top3_locations', value=locations_str)



    print_top3_locations = PythonOperator(
        task_id='print_top3_locations',
        python_callable=load_ram_func
    )


    def load_locations_to_gp_func(**kwargs):
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.run("truncate table n_kotsjuba_5_ram_location", False)
        #val = kwargs['ti'].xcom_pull(task_ids='print_top3_locations', key='top3_locations')
        val = kwargs['ti'].xcom_pull(task_ids='print_top3_locations_op', key='top3_locations')
        sql_statement = "INSERT INTO n_kotsjuba_5_ram_location VALUES "+val
        pg_hook.run(sql_statement, False)

    load_locations_to_gp = PythonOperator(
        task_id="load_locations_to_gp",
        python_callable=load_locations_to_gp_func,
        provide_context=True
    )

    print_top3_locations_op = KotsjubaRamResidentCountOperator(
        task_id='print_top3_locations_op'
    )

    start >> print_top3_locations_op >> load_locations_to_gp
