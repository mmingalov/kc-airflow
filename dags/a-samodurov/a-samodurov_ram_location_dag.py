from datetime import timedelta, datetime
from airflow import DAG
from a_samodurov_plugins.a_samodurov_ram_location_operator import TopLocationsSamodurovOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'a-samodurov',
    'start_date': datetime(2022, 3, 1),
    'depends_on_past': False
}
conn_id = 'conn_greenplum_write'
table_name = 'a_samodurov_ram_location'

with DAG(
        "a-samodurov_top_loc",
        schedule_interval='@daily',
        default_args=default_args,
        max_active_runs=1,
        tags=['a-samodurov']
        ) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql=f"""
            create table if not exists {table_name} (
            id int8,
            name varchar(100),
            type varchar(100),
            dimension varchar(100),
            resident_cnt int8,
            primary key(id)
            );
        """,
        autocommit=True,
        postgres_conn_id='conn_greenplum_write',
        dag=dag
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id=conn_id,
        sql=f"truncate table {table_name};",
        autocommit=True,
        dag=dag
    )

    export_top_locations = TopLocationsSamodurovOperator(
        task_id='export_table',
        top=3,
        table_name=table_name
    )

create_table >> truncate_table >> export_top_locations