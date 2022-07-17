from airflow import DAG, AirflowException
from datetime import datetime
from plugins.v_aleshin_plugins.valeshin_ram_operators import RamTop3LocationOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

URL = 'https://rickandmortyapi.com/api/location/'

DEFAULT_ARGS = {
    'start_date': datetime(2022, 5, 14),
    'owner': 'v-aleshin'
}


def _load_locations_to_gp(**kwargs):
    locations_list = kwargs['ti'].xcom_pull(task_ids='get_top_3_locations_from_api',
                                            key='rick_and_morty_top_3_location')

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE students.public.v_aleshin_ram_location;')
                for location in locations_list:
                    cur.execute(
                        'INSERT INTO students.public.v_aleshin_ram_location(id, name, type, dimension, resident_cnt) '
                        'values (%s, %s)',
                        (location.id, location.name, location.type, location.dimension, location.resident_cnt))
    finally:
        conn.close()


with DAG(
        dag_id='valeshin_ram',
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['valeshin']
) as dag:
    get_top_3_locations_from_api = RamTop3LocationOperator(
        task_id='get_top_3_locations_from_api',
        url=URL
    )
    load_Locations_to_gp = PythonOperator(
        task_id='load_locations_to_gp',
        python_callable=_load_locations_to_gp
    )
    get_top_3_locations_from_api >> load_Locations_to_gp
