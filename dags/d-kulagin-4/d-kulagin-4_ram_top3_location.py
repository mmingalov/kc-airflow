"""
Finds top 3 locations by residents count from rickandmortyapi.com
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # noqa
from airflow.utils.dates import days_ago

from d_kulagin_4_plugins.ram_locations_op import RAMLocationsOp

_DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'd-kulagin-4',
    'poke_interval': 600
}


def upload_to_gp(insert_values):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS d_kulagin_4_ram_location (
                id int, name text, type text, dimension text, resident_cnt int
            );
        ''')
        cur.execute('''
            TRUNCATE TABLE d_kulagin_4_ram_location;
        ''')
        cur.execute(f'''
            INSERT INTO d_kulagin_4_ram_location (id, name, type, dimension, resident_cnt)
            VALUES {insert_values};
        ''')

    conn.commit()
    conn.close()


with DAG("d-kulagin-4_ram_top3_locations",
         schedule_interval='@daily',
         default_args=_DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-kulagin-4', 'ram_top3_locations']) as dag:

        start = DummyOperator(task_id='start', dag=dag)
        ram_location_op = RAMLocationsOp(task_id='ram_top3_locations', dag=dag, do_xcom_push=True)
        gp_upload = PythonOperator(
            task_id='gp_upload',
            python_callable=upload_to_gp,
            op_args=["{{ task_instance.xcom_pull(task_ids='ram_top3_locations', key='return_value') }}",],
        )

        start >> ram_location_op >> gp_upload
