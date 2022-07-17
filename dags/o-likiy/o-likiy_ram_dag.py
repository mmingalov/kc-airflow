import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from o_likiy_plugins.olikiy_ram_plugin import OLikiyRAMTop3LocationOperator
from airflow.utils.dates import days_ago


default_args = {
    'start_date': days_ago(2),
    'owner': 'o-likiy',
    'poke_interval': 600
}

with DAG(
    dag_id='o-likiy_ram_dag',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    tags=['o-likiy'],
) as dag:

    start_task = DummyOperator(task_id='start_task')

    create_table_gp_task = PostgresOperator(task_id="create_table_gp_task",
                                            postgres_conn_id="conn_greenplum_write",
                                            sql="""
                                                     CREATE TABLE IF NOT EXISTS olikiy_ram_location (
                                                     id integer PRIMARY KEY,
                                                     name varchar not null,
                                                     type varchar not null,
                                                     dimension varchar not null,
                                                     resident_cnt integer) DISTRIBUTED BY (id);
                                                     TRUNCATE TABLE olikiy_ram_location;
                                                """,
                                            autocommit=True,)

    top3_location_task = OLikiyRAMTop3LocationOperator(task_id='top3_location_task')

    end_task = DummyOperator(task_id='end_task')

    start_task >> create_table_gp_task >> top3_location_task >> end_task
