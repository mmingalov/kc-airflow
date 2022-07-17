"""
Домашнее задание - Rick and Morty
"""
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
# import datetime
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from i_trubikov_plugins.i_trubnikov_operator import i_trubnikov_LocCountOperator
# from i_trubikov_plugins.i_trubnikov_operator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i.trubnikov',
    'poke_interval': 600
}

TABLE_NAME = 'i_tru_ram_location'
GREENPLUM_CONN = 'conn_greenplum_write'


with DAG("i.trubnikov_ram",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i.trubnikov']
) as dag:
    dummy = DummyOperator(task_id="dummy")

    get_top_locations_task = i_trubnikov_LocCountOperator(
        task_id='get_ram_data_and_transform'

    )


    def write_to_greenplum(**context):
        top_df = pd.read_csv('tmp/i.trubnikov/RAM_top_loc.csv',index_col='id')
        logging.info(top_df)

        logging.info("Информация получена")
        postgres_hook = PostgresHook(postgres_conn_id=GREENPLUM_CONN)
        engine = postgres_hook.get_sqlalchemy_engine()
        logging.info("Соединение установленно")
        top_df.to_sql(name=TABLE_NAME, con=engine,
                  if_exists='replace', index=False)

        # logging.info(top_df.to_sql(name=TABLE_NAME, con=engine,
        #           if_exists='replace', index=False))

        engine.dispose()
        logging.info("Таблица успешно записана")

    def write_to_greenplum_2(**context):
        top_df = pd.read_csv('tmp/i.trubnikov/RAM_top_loc.csv', index_col='id')
        connection = BaseHook.get_connection("conn_greenplum_write")
        param_dic = {
            "host": "greenplum.lab.karpov.courses",
            "database": "karpovcourses",
            "user": 'student',
            "password": 'Wrhy96_09iPcreqAS'
        }
        connect = "postgresql+psycopg2://%s:%s@%s:6432/%s" % (
            param_dic['user'],
            param_dic['password'],
            param_dic['host'],
            param_dic['database']
        )

        # def write_to_greenplum_3(**context):
        #     pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        #     conn = pg_hook.get_conn()  # берём из него соединение
        #     cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        #     cursor.execute('creat')  # исполняем sql

        engine = create_engine(connect)
        top_df.to_sql(
            f'{TABLE_NAME}',
            # con=engine,
            con=engine,
            index=False,
            if_exists='replace'
        )


    greenplum_task = PythonOperator(
        task_id='greenplum_task',
        python_callable=write_to_greenplum
    )

    write_to_sql_2 = PythonOperator(
        task_id='write_to_sql',
        python_callable=write_to_greenplum_2
    )



    dummy >> get_top_locations_task >> greenplum_task >> write_to_sql_2

