from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from p_kuznetsov_7_plugins.p_kuznetsov_7_residents import RamTopThreeLocations


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'p-kuznetsov-7',
    'poke_interval': 600
}

with DAG("p_kuznetsov_7_RAM_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p-kuznetsov-7']
         ) as dag:

    dummy = DummyOperator(task_id='dummy')

    top_3_location_operator = RamTopThreeLocations(
         task_id='get_top_3_location'
    )


    def write_csv_func(ti):
        top_locations = ti.xcom_pull(key='return_value', task_ids='get_top_3_location')
        logging.info(top_locations)
        keys = top_locations[0].keys()
        logging.info('Top-3 locations is:')
        logging.info(top_locations)
        with open('/tmp/p_kuznetsov_7_RAM_data.csv', 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)

            dict_writer.writerows(top_locations)


    write_csv = PythonOperator(
        task_id='write_csv',
        python_callable=write_csv_func,
        dag=dag
    )


    def load_to_greenplum_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        truncate_table = """TRUNCATE TABLE p_kuznetsov_7_ram_location"""
        pg_hook.run(truncate_table, False)
        pg_hook.copy_expert("COPY p_kuznetsov_7_ram_location FROM STDIN DELIMITER ','",
                            '/tmp/p_kuznetsov_7_RAM_data.csv')


    load_csv_to_greenplum = PythonOperator(
        task_id='load_to_greenplum',
        python_callable=load_to_greenplum_func,
        dag=dag
    )

    dummy >> top_3_location_operator >> write_csv >> load_csv_to_greenplum
