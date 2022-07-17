"""
Записываем в таблицу топ-3 локаций
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

from d_grigorev_plugins.d_grigorev_ram_top3 import Ram_Top3_loc

import json
import logging


DEFAULT_ARGS = {
    "start_date": days_ago(1),
    'owner': 'd-grigorev',
    'poke_interval': 600
}

with DAG("d-grigorev-ram-dag",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-grigorev'],
         catchup=True
         ) as dag:


    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        with pg_hook.get_conn() as conn:
            curs = conn.cursor()
            curs.execute(f"""SELECT * FROM public."d-grigorev_ram_location";""")
            fetch = curs.fetchall()
            logging.info(f"Check length of fetch: {len(fetch[0])}")
            if fetch:
                curs.execute(f"""TRUNCATE table public."d-grigorev_ram_location";""")
                logging.info("Deleting table")
                conn.commit()
                curs.execute(f"""
                                create table if not exists "d-grigorev_ram_location" (
                                id integer,
                                name varchar,
                                type varchar,
                                dimension varchar,
                                resident_cnt integer 
                                );
                                """
                                        )
                conn.commit()
                curs.close()
            else:
                curs.execute(f"""
                                create table "d-grigorev_ram_location" (
                                id integer,
                                name varchar,
                                type varchar,
                                dimension varchar,
                                resident_cnt integer 
                                );
                                """
                                        )
                conn.commit()
                logging.info("Table created")
                curs.close()

    def update_table(**kwargs):
        top = kwargs['ti'].xcom_pull(key='d-grigorev-ram-location')
        logging.info(f"Get top locations {top}")
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        with pg_hook.get_conn() as conn:
            curs = conn.cursor()
            for key in top:
                val = (top[key]['id'], top[key]['name'], top[key]['type'], top[key]['dimension'], top[key]['resident_cnt'])
                logging.info(f"value {val}")
                curs.execute(
                    f"""
                    insert into "d-grigorev_ram_location" (
                        id,
                        name,
                        type,
                        dimension,
                        resident_cnt
                    )
                    values (%s, %s, %s, %s, %s)
                """, val
                )

                conn.commit()
            curs.close()
        logging.info("SUCCESS")


    create_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        provide_context=True,
        dag=dag
    )

    get_top3_locations = Ram_Top3_loc(
        task_id="get_top3_locations"
    )


    update_task = PythonOperator(
        task_id='update_table',
        python_callable=update_table,
        provide_context=True,
        dag=dag
    )


    create_task >> get_top3_locations >> update_task
