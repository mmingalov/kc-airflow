"""
Сергей Коноплев
RAM, задание 7
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import pandas as pnds

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from s_konoplev_3_plugins.s_konoplev_3_ram_operator import SKonoplev3Operator

TABLE_NAME = 'konoplev_location_top3'
DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 's-konoplev-3',
    'poke_interval': 600
}

dag = DAG("sk_test_dag_ram",
          schedule_interval='@once',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-konoplev-3']
          )

start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

get_ram_locations_info = SKonoplev3Operator(
        task_id='get_ram_locations_info',
        do_xcom_push=True,
        dag=dag
    )

def tr_data_func(task_instance):
    rows = task_instance.xcom_pull(
        task_ids='get_ram_locations_info',
        key='return_value'
    )
    logging.info(f"Rows: {str(rows)}")
    sort_result = sorted(rows, key=lambda x: x[4], reverse=True)
    rows2 = sort_result[:3]
    logging.info(f"Rows2: {str(rows2)}")
    return rows2

def save_data_to_db_func(task_instance):
    rows2 = task_instance.xcom_pull(
        task_ids='tr_ram_locations_info',
        key='return_value'
    )

    query_str = f"""INSERT INTO {TABLE_NAME} (id, name, type, dimension, resident_cnt) values (%s, %s, %s, %s, %s)"""

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
           id INT PRIMARY KEY,
           name text,
           type text,
           dimension text,
           resident_cnt INT NOT NULL
        );
        TRUNCATE {TABLE_NAME};
        """)

    for r in rows2:
        logging.info(f"Row to DB: {str(r)}")
        cursor.execute(query_str, r)
    conn.commit()
    cursor.close()
    conn.close()

tr_ram_locations_info = PythonOperator(
    task_id='tr_ram_locations_info',
    python_callable=tr_data_func,
    do_xcom_push=True,
    dag=dag
)

save_ram_locations_info = PythonOperator(
    task_id='save_data_to_db',
    python_callable=save_data_to_db_func,
    dag=dag
)

start_task >> get_ram_locations_info >> tr_ram_locations_info >> save_ram_locations_info >> end_task

dag.doc_md = __doc__

get_ram_locations_info.doc_md = """Выгружает данные RAM (E step)'"""
tr_ram_locations_info.doc_md = """Преобразует данные RAM (T step)'"""
save_ram_locations_info.doc_md = """Сохраняет данные RAM (L step)'"""
end_task.doc_md = """Начинает DAG'"""
end_task.doc_md = """Завершает DAG'"""
