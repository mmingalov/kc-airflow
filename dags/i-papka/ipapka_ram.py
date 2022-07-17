"""
Загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago


from airflow.operators.dummy import DummyOperator

from ipapka_plugins.ipapka_ram_species_count_operator import IpapkaRamSpeciesCountOperator



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-papka',
    'poke_interval': 600
}


with DAG("ipapka_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-papka']
         ) as dag:

    start = DummyOperator(task_id='start')

    print_alien_count = IpapkaRamSpeciesCountOperator(
        task_id='print_alien_count',
        species_type='Alien'
    )

    start >> print_alien_count