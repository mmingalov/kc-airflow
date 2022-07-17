import logging
import json
from datetime import datetime
from contextlib import closing

from airflow import DAG
from jdylko3_plugins.jdylko3_ram_locations_operator import JDylko3RaMLocationsOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator


def check_if_table_exists_func(**kwargs):
    """Check if table exists in GreenPlum and select the corresponding task to proceed"""
    query = """
        SELECT EXISTS (
            SELECT * FROM information_schema.tables
            WHERE table_schema = '{0}'
              AND table_name = '{1}'
        )
    """.format(kwargs['schema'], kwargs['table'])

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    with closing(pg_hook.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            logging.info(f"Execute query: {query}")
            cur.execute(query)
            result = cur.fetchall()
            if result[0][0]:
                logging.info(f"Table {kwargs['schema']}.{kwargs['table']} exists.")
                return 'load_data_to_gp'
            else:
                logging.info(f"Table {kwargs['schema']}.{kwargs['table']} exists.")
                return 'create_gp_table'


def load_data_to_gp_func(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    locations = json.loads(kwargs['ti'].xcom_pull(task_ids='get_top3_RaM_locations', key=kwargs['xcom_key']))
    logging.info(f'Pulled locations from XCom: {json.dumps(locations)}')

    with closing(pg_hook.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            for location in locations:
                # check if location id exists in table:
                check_id_query = "SELECT EXISTS (SELECT 1 FROM {0}.{1} WHERE id = {2})".format(kwargs['schema'],
                                                                                               kwargs['table'],
                                                                                               location['id'])
                logging.info(f"Checking query:\n{check_id_query}")
                cur.execute(check_id_query)
                result = cur.fetchall()
                if result[0][0]:
                    # overwrite existing location id with new data
                    logging.info(f"Location with id = {location['id']} exists in table. ")
                    logging.info(f"Overwrite data for this location with new data: {location}")
                    update_query = """
                        UPDATE {schema}.{table}
                        SET (name, type, dimension, resident_cnt) = ('{name}', '{type}', '{dimension}', {resident_cnt})
                        WHERE id = {id};
                    """.format(schema=kwargs['schema'], table=kwargs['table'], **location)
                    logging.info(f"Update query:\n{update_query}")
                    cur.execute(update_query)
                    if cur.rowcount >= 0:
                        logging.info(f"Rows affected: {cur.rowcount}")
                else:
                    logging.info(f"Insert new row with location data: {location}")
                    insert_query = """
                        INSERT INTO {schema}.{table}
                        VALUES ({id}, '{name}', '{type}', '{dimension}', {resident_cnt});
                    """.format(schema=kwargs['schema'], table=kwargs['table'], **location)
                    logging.info(f"Insert query:\n{insert_query}")
                    cur.execute(insert_query)
                    if cur.rowcount >= 0:
                        logging.info(f"Rows affected: {cur.rowcount}")


default_args = {
    'owner': 'j-dylko-3',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2021, 12, 15),
    'end_date': datetime(2021, 12, 16),
    'retries': 2,
}

with DAG(
        dag_id='j-dylko-3-dag-two',
        default_args=default_args,
        schedule_interval='@once',
        description='Даг по домашнему заданию №2',
        max_active_runs=1,
        concurrency=2
) as main_dag:

    get_top3_RaM_locations = JDylko3RaMLocationsOperator(
        task_id='get_top3_RaM_locations',
        xcom_key='top3_locations',
    )

    check_if_table_exists = BranchPythonOperator(
        task_id='check_if_table_exists',
        python_callable=check_if_table_exists_func,
        op_kwargs={
            'schema': 'public',
            'table': 'j_dylko_3_ram_location'
        },
    )

    create_gp_table = PostgresOperator(
        task_id='create_gp_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS public.j_dylko_3_ram_location (
                id INTEGER UNIQUE NOT NULL,
                name TEXT,
                type TEXT,
                dimension TEXT,
                resident_cnt INTEGER,
                UNIQUE(id, name, type, dimension, resident_cnt)
            ) DISTRIBUTED BY (id);
        """
    )

    load_data_to_gp = PythonOperator(
        task_id='fetch_data_from_gp',
        python_callable=load_data_to_gp_func,
        op_kwargs={
            'schema': 'public',
            'table': 'j_dylko_3_ram_location',
            'xcom_key': 'top3_locations'
        },
        provide_context=True
    )

    get_top3_RaM_locations >> check_if_table_exists >> [create_gp_table, load_data_to_gp]
    create_gp_table >> load_data_to_gp
