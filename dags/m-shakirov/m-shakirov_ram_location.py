import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from m_shakirov_plugins.m_shakirov_ram_location_operator import (
    M_Shakirov_Hook_Location_Operator,
)

from airflow import DAG

TABLE_NAME = "m_shakirov_ram_location"

DEFAULT_ARGS = {
    "owner": "m-shakirov",
    "start_date": days_ago(2),
}

with DAG(
    "m-shakirov_ram_location",
    schedule_interval="@once",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["m-shakirov"],
) as dag:

    def create_table_func():
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        conn = pg_hook.get_conn()

        cursor = conn.cursor()
        table_create_query =  f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id integer NOT null,
                name varchar,
                type varchar,
                dimension varchar,
                resident_cnt integer,
                primary key (id)
            );
        """
        cursor.execute(table_create_query)
        conn.commit()

        cursor.close()
        conn.close()

    create_table = PythonOperator(
        task_id="create_table", python_callable=create_table_func
    )

    get_top3_locations = M_Shakirov_Hook_Location_Operator(
        task_id="get_top3_locations",
        cnt=3,
    )

    def write_top_to_db_func(**context):
        top_locations = context["ti"].xcom_pull(key="m_shakirov_ram_locations")
        logging.info(f"Top locations from XCOM: {top_locations}")

        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        conn = pg_hook.get_conn()

        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE {TABLE_NAME}")
        conn.commit()

        args_str = ",".join(
            f"""({location["id"]}, '{location["name"]}', '{location["type"]}', '{location["dimension"]}', {location["resident_cnt"]})"""
            for location in top_locations
        )

        logging.info(f'values: {args_str}')

        insert_query = f"""
            INSERT INTO {TABLE_NAME} (
                id,
                name,
                type,
                dimension,
                resident_cnt
            )
            VALUES {args_str}  
        """
        cursor.execute(insert_query)
        conn.commit()

        cursor.close()
        conn.close()

    write_top_to_db = PythonOperator(
        task_id="write_top_to_db",
        provide_context=True,
        python_callable=write_top_to_db_func,
    )

    create_table >> get_top3_locations >> write_top_to_db
