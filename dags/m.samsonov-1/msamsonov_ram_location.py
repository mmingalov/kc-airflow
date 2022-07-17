import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from msamsonov_plugins.msamsonov_ram_location_operator import (
    MSamsonovRAMLocationOperator,
)

from airflow import DAG

TABLE_NAME = "msamsonov_ram_location"

DEFAULT_ARGS = {
    "owner": "msamsonov",
    "start_date": days_ago(2),
}

with DAG(
    "msamsonov_ram_location",
    schedule_interval="@once",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["msamsonov"],
) as dag:

    def create_table_func():
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        conn = pg_hook.get_conn()

        cursor = conn.cursor()
        cursor.execute(
            f"""
            create table if not exists {TABLE_NAME} (
                id integer not null,
                name varchar,
                type varchar,
                dimension varchar,
                resident_cnt integer,
                primary key (id)
            );
        """
        )
        conn.commit()

        cursor.close()
        conn.close()

    create_table = PythonOperator(
        task_id="create_table", python_callable=create_table_func
    )

    get_top3_locations = MSamsonovRAMLocationOperator(
        task_id="get_top3_locations",
        cnt=3,
    )

    def write_top_to_db_func(**context):
        top_locations = context["ti"].xcom_pull(key="msamsonov_ram_locations")
        logging.info(f"Top locations from xcom: {top_locations}")

        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        conn = pg_hook.get_conn()

        cursor = conn.cursor()
        cursor.execute(f"truncate {TABLE_NAME}")
        conn.commit()

        args_str = ",".join(
            f"""({location["id"]}, '{location["name"]}', '{location["type"]}', '{location["dimension"]}', {location["resident_cnt"]})"""
            for location in top_locations
        )

        logging.info(f'values: {args_str}')

        cursor.execute(
            f"""
            insert into {TABLE_NAME} (
                id,
                name,
                type,
                dimension,
                resident_cnt
            )
            values {args_str}  
        """
        )
        conn.commit()

        cursor.close()
        conn.close()

    write_top_to_db = PythonOperator(
        task_id="write_top_to_db",
        provide_context=True,
        python_callable=write_top_to_db_func,
    )

    create_table >> get_top3_locations >> write_top_to_db
