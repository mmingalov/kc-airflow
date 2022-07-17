"""
RAM API
С помощью API (https://rickandmortyapi.com/documentation/location) находит три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Создает таблицу в GreenPlum'е таблицу с названием "public.p_kozhin_ram_location" с полями id, name, type, dimension, resident_cnt;
resident_cnt — длина списка в поле residents.
Записывает значения соответствующих полей этих трёх локаций в таблицу
"""

import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from p_kozhin.p_kozhin_ram_top_location_operator import KozhinRickMortyHook
from p_kozhin.p_kozhin_ram_top_location_operator import KozhinRamTopLocationByResidentsCountOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'p.kozhin-2',
    'poke_interval': 600
}

with DAG("p.kozhin-2_RAM",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p.kozhin-2']
         ) as dag:

    def _count_top3_location_by_resident_func(**context):
        """
        return top location tuples by residents count (position 4 in tuple ) in residents_in_locations list of tuple
        """
        residents_in_locations = context['ti'].xcom_pull(task_ids="get_ram_locations_with_residents")
        top = sorted(residents_in_locations, key=lambda x: x[4], reverse=True)[:3]
        return top


    count_top3_locations_by_residents = PythonOperator(
        task_id="count_top3_locations_by_residents",
        python_callable=_count_top3_location_by_resident_func,
        dag=dag
    )

    def _write_top_location_to_gp_func(**context):
        top = context['ti'].xcom_pull(task_ids="count_top3_locations_by_residents")
        logging.info('writing TOP to public.p_kozhin_ram_location')
        # подключени к Greenplum
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()
        cursor.execute('''DROP TABLE IF EXISTS public.p_kozhin_ram_location;
                        CREATE TABLE public.p_kozhin_ram_location
                        (id INT, name text, type text, dimension text, resident_cnt INT)
                        ''')
        conn.commit()
        logging.info('public.p_kozhin_ram_location created')
        pg_hook.insert_rows('public.p_kozhin_ram_location',
                            top,
                            target_fields=['id', 'name', 'type', 'dimension', 'resident_cnt'],
                            commit_every=0)
        conn.commit()
        conn.close()
        logging.info(f'{top} rows inserted to public.p_kozhin_ram_location')

    write_top_location_to_gp = PythonOperator(
            task_id="write_top_location_to_gp",
            python_callable=_write_top_location_to_gp_func,
            dag=dag
        )

    get_ram_locations_with_residents = KozhinRamTopLocationByResidentsCountOperator(
        task_id="get_ram_locations_with_residents"
    )

    get_ram_locations_with_residents >> count_top3_locations_by_residents >> write_top_location_to_gp

    dag.doc_md = __doc__

    get_ram_locations_with_residents = """
        Получает значение id, name, type, dimension, resident_cnt для каждой location в RAM  
        Передает их в XCOM. (пока значений не много можно использовать XCOM)
        """
    count_top3_locations_by_residents = """Вычисляет TOP3 локации с максимальным значением resident_cnt
        Передает значение в XCOM
        """
    write_top_location_to_gp = """записывает значения TOP3 локации в greenplum"""