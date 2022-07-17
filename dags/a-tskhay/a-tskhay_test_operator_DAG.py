from airflow import DAG

from airflow.utils.dates import days_ago
from a_tskhay_plugins.a_tskhay_operator_test import DinaRamSpeciesCountOperator
#from dina_plugins.dina_ram_species_count_operator import DinaRamSpeciesCountOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-tskhay'
}

with DAG("a-tskhay_test_operator_DAG",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['a-tskhay']
         ) as dag:

    print_alien_count = DinaRamSpeciesCountOperator(
        task_id='print_alien_count',
        species_type='Alien'
    )

    print_human_count = DinaRamSpeciesCountOperator(
        task_id='print_human_count',
        species_type='Human'
    )

    [print_alien_count, print_human_count]