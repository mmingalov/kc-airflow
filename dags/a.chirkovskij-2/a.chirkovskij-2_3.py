"""
Дагер, хранящий информациюю о локациях из Рика и морти.
Создает таблицу, если ее ещё нет.
Получает через специальный оператор топ-3 локации из апи
Выкидывает из таблицы (если она есть) все записи с айдишниками, которые пришли
Записывает топ-3 локации в таблицу.
Так как таблица чистится не полностью, а только по айдишникам новых записей (во избежание дублей),
Гипотетически возможно, что, если данные поменяются (допустим, в топ-3 попадет какая-то другая лока), 
В таблице станет четыре записи, а не три, а потом -- больше.
Хотя в задании не сказано, что в таблице всегда должны быть только актуальные данные.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from a_chirkovskij_2_plugins.ram_location_operator import RAM_TOP3_Operator
import random

DEFAULT_ARGS = {
    'owner': 'a.chirkovskij-2',
    'start_date': days_ago(2),
    'poke_interval': 600,
}


with DAG("a.chirkovskij-2_3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a.chirkovskij-2']
         ) as dag:

    start = DummyOperator(task_id='start')
    
    connection = 'conn_greenplum_write'
    table_name = 'a_chirkovskij_2_ram_location'
 
     
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=connection,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            dimension VARCHAR,
            resident_cnt INTEGER NOT NULL);
          """,
    )
    
    #def create_table_f(**kwargs):
    #    sql="""
    #        CREATE TABLE IF NOT EXISTS a_chirkovskij_2_ram_location (
    #        id INTEGER PRIMARY KEY,
    #        name VARCHAR NOT NULL,
    #        type VARCHAR NOT NULL,
    #        dimension VARCHAR NOT NULL,
    #        resident_cnt INTEGER NOT NULL);
    #        """
    #    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    #    pg_hook.run(sql)
        
    #create_table = PythonOperator(
    #    task_id='create_table',
    #    python_callable=create_table_f,
    #    provide_context=True
    #    )
    
    #def extract_data_f(**kwargs):
    #    ret = [{'id': 0, 'name': 'Foofel', 'type': 'Foofelland', 'dimension': 'Norilskgrad', 'resident_cnt': 1}]
    #    logging.info(ret)
    #    kwargs['ti'].xcom_push(value=ret, key='data')

    #extract_data = PythonOperator(
    #    task_id='extract_data',
    #    python_callable=extract_data_f,
    #    provide_context=True
    #    )
    
    extract_data = RAM_TOP3_Operator(
        task_id='extract_data',
        )
        
    def insert_data_f(**kwargs):
        data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='return_value')
        pg_hook = PostgresHook(postgres_conn_id=connection)
        
        for d in data:
            pg_hook.run(f'DELETE FROM {table_name} WHERE id = {d["id"]};')
                
            pg_hook.run(f'INSERT INTO {table_name} (id, name, type, dimension, resident_cnt) VALUES ( {d["id"]}, \'{d["name"]}\', \'{d["type"]}\', \'{d["dimension"]}\', {d["resident_cnt"]});')

        
    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data_f,
        provide_context=True
        )    

    
    start >> create_table >> extract_data >> insert_data
