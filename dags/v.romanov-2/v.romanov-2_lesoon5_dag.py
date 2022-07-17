import logging
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from vromanov_plugins.vromanov_ram_top3_residents_location import RomanovRAMTopLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v.romanov-2',
    'poke_interval': 600
}

with DAG("v.romanov-2_lesson5_dag",
         schedule_interval='0 0 * * MON-SAT',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v.romanov-2']
         ) as dag:
    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    def create_ram_table_func(**kwags):
        sql_create_table = "CREATE TABLE IF NOT EXISTS v_romanov_2_ram_location(id integer primary key, name varchar(50), type varchar(50) , dimension varchar(50) , resident_cnt integer)"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(sql_create_table, False)

    def save_top_location_func(**kwags):
        ram_top_locations_list = kwags["ti"].xcom_pull(task_ids="ram_top", key="ram_top_locations")
        values = ",".join("{}".format(d) for d in ram_top_locations_list)
        sql_statement ="INSERT INTO public.v_romanov_2_ram_location(id, name, type , dimension, resident_cnt) VALUES {}".format(values)
        logging.info(f'Executing {sql_statement}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(sql_statement, False)


    create_ram_table = PythonOperator(
        task_id='create_ram_table',
        python_callable=create_ram_table_func,
        provide_context=True
    )
    ram_top_locations = RomanovRAMTopLocationOperator(task_id="ram_top")

    save_top_locations = PythonOperator(
        task_id='save_top_location',
        python_callable=save_top_location_func,
        provide_context=True
    )

    start >> create_ram_table >> ram_top_locations >> save_top_locations >> end
