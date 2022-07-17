import logging

import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from d_zhirnov_8_plugins.operators.DZhirnovRickAndMortiCount import DZhirnovRickAndMortiCountOperator
from dateutil.utils import today
DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'd.zhirnov-8',
    'poke_interval' : 600
}
with DAG(
    dag_id='d.zhirnov-8_top_3',
    #schedule_interval='0 6 * * 1-6',
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['DE']
) as dag:

    # def get_data_from_url(api_url='https://rickandmortyapi.com/api/location', **kwargs):
    #     """
    #     Get count of page in API
    #     :param api_url
    #     :return: page count
    #     """
    #     r = requests.get(api_url)
    #     if r.status_code == 200:
    #         logging.info("SUCCESS")
    #         data = r.json().get('results')
    #         logging.info(f'page_count = {len(data)}')
    #         kwargs['ti'].xcom_push(value=data, key='data')
    #         #return data
    #     else:
    #         logging.warning("HTTP STATUS {}".format(r.status_code))
    #         raise AirflowException('Error in load page count')
    #         #print('error')



    #def get_top_3(**kwargs):
    #    df = kwargs['ti'].xcom_pull(key='data')
    #    residents_df = pd.DataFrame(df)
    #    residents_df['resident_cnt'] = residents_df.residents.apply(lambda x: len(x))
    #    residents_df.sort_values('resident_cnt', ascending=False, inplace=True)
    #    logging.info(residents_df.head(3))
    #    residents_df[['id', 'name', 'type', 'dimension', 'resident_cnt']].head(3).to_csv('resident.csv', sep=',', index=False, header=False)
        #return residents_df[['id', 'name', 'type', 'dimension', 'resident_cnt']].head(3)
    def create_greenplum_table():
        pg = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg.run("""CREATE TABLE  if not exists public.d_zhirnov_8_ram_location (
        id int4 null primary KEY,
        "name" varchar(1024) NULL,
        "type" varchar(1024) NULL,
        dimension text NULL,
        resident_cnt int4 NULL
        )
        DISTRIBUTED BY (id);""")
    def write_data_to_greenplum_table():
        pg = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg.run("""DELETE FROM public.d_zhirnov_8_ram_location""")
        logging.info(pd.read_csv('resident.csv'))
        pg.copy_expert("Copy d_zhirnov_8_ram_location  FROM STDIN Delimiter ','", 'resident.csv')


    get_top_n = DZhirnovRickAndMortiCountOperator(
        task_id='get_top_n',
        top_n=3
    )
    # get_data_from_url = PythonOperator(
    #     task_id='get_data_from_url',
    #     python_callable=get_data_from_url
    #
    # )
    create_greenplum_table = PythonOperator(
        task_id='create_greenplum_table',
        python_callable=create_greenplum_table
    )
    write_data_to_greenplum_table = PythonOperator(
        task_id='write_data_to_greenplum_table',
        python_callable=write_data_to_greenplum_table
    )
    #get_data_from_url>>create_greenplum_table>>get_top_3>>write_data_to_greenplum_table
    create_greenplum_table >> get_top_n >> write_data_to_greenplum_table
