"""
В процессе выполнения этого задания я понизил
свой рейтинг и рейтинг мультика в русообществе
датасаентистов.. =)
"""


import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow import DAG
import pandas as pd
from sqlalchemy import create_engine
from de_rpuropuu.ramapi import Location as rm
from airflow.hooks.base_hook import BaseHook  # Deprecated in Airflow 2


### начало дага и его описание
with DAG(dag_id='rpuropuu_dag_003',
         default_args={'owner': 'rpuropuu',
                       'retries': 5,
                       'poke_interval': 30
                       },
         schedule_interval="0 0 * * 1-6",
         max_active_runs=1,
         start_date=days_ago(1),
         tags=['excercise_003']
    ) as dag:


    def go_take_it():
        # count of all dimensions
        dimension = rm.get_all()['info']['count']
        # creating full data frame
        df_original = []
        for i in range(dimension):
            df_original.append([rm.get(i+1)['id'],\
                                rm.get(i+1)['name'],\
                                rm.get(i+1)['type'],\
                                rm.get(i+1)['dimension'],\
                                len(rm.get(i+1)['residents']),\
                                ])
        return df_original


    def write_to_sql_use_connection(**kwargs):
        """
        Using a dummy table to test this call library
        """
        locations = kwargs['ti'].xcom_pull(task_ids='taking_info')
        # set column names
        topic = ['id', 'name', 'type', 'dimension', 'resident_cnt']
        df_original = pd.DataFrame(locations, columns=topic)
        # order by resident_cnt
        df_original = df_original.sort_values('resident_cnt', ascending=False)
        # final step - top 3 locations
        df = df_original.head(3)

        # get connection param
        connection = BaseHook.get_connection("conn_greenplum_write")
        param_dic = {
        "host"      : "greenplum.lab.karpov.courses",
        "database"  : "students",
        "user"      : str(connection.login),
        "password"  : str(connection.password)
        }
        connect = "postgresql+psycopg2://%s:%s@%s:6432/%s" % (
            param_dic['user'],
            param_dic['password'],
            param_dic['host'],
            param_dic['database']
        )
        engine = create_engine(connect)
        df.to_sql(
            'rpuropuu_ram_location',
            # con=engine,
            con=engine,
            index=False,
            if_exists='replace'
        )


    taking_info = PythonOperator(
        task_id='taking_info',
        python_callable=go_take_it,
        do_xcom_push=True
    )


    write_to_sql = PythonOperator(
        task_id='write_to_sql',
        python_callable=write_to_sql_use_connection
    )

    taking_info >> write_to_sql

