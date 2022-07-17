"""
Загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from dina_plugins.dina_ram_species_count_operator import DinaRamSpeciesCountOperator
from dina_plugins.dina_ram_dead_or_alive_operator import DinaRamDeadOrAliveCountOperator
from dina_plugins.dina_random_sensor import DinaRandomSensor


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'Karpov',
    'poke_interval': 600
}


with DAG("dina_load_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    start = DummyOperator(task_id='start')

    random_wait = DinaRandomSensor(task_id='random_wait', mode='reschedule')

    def get_page_count(api_url):
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')


    def get_human_count_on_page(result_json):
        """
        Get count of human in one page of character
        :param result_json:
        :return: human_count
        """
        human_count_on_page = 0
        for one_char in result_json:
            if one_char.get('species') == 'Human':
                human_count_on_page += 1
        logging.info(f'     human_count_on_page = {human_count_on_page}')
        return human_count_on_page


    def load_ram_func():
        """
        Logging count of Human in Rick&Morty
        """
        human_count = 0
        ram_char_url = 'https://rickandmortyapi.com/api/character?page={pg}'
        for page in range(get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                human_count += get_human_count_on_page(r.json().get('results'))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        logging.info(f'Humans in Rick&Morty: {human_count}')

    print_human_count = PythonOperator(
        task_id='print_human_count',
        python_callable=load_ram_func
    )

    print_alien_count = DinaRamSpeciesCountOperator(
        task_id='print_alien_count',
        species_type='Alien'
    )

    print_dead_count = DinaRamDeadOrAliveCountOperator(
        task_id='print_dead_count',
        dead_or_alive='Dead'
    )

    start >> random_wait >> [print_human_count, print_alien_count, print_dead_count]
