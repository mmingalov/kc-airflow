"""даг для My operators"""

from airflow import DAG
from datetime import datetime
from a_shumilov_4_plugins.operators.shumilov_operators_for_ram import ShumilovCreateDatabaseOperator, ShumilovInsertDataOperator



DEFAULT_ARGS = {
    'start_date': datetime.now(),
    'owner': 'a-shumilov-4',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': 10,
    'priority_weight': 2

}

dag = DAG("a-shumilov-4_dag5_myoperators",
          schedule_interval='@once',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-shumilov-4']
          )

create_database = ShumilovCreateDatabaseOperator(
    task_id='create_database',
    dag=dag
)

insert_data = ShumilovInsertDataOperator(
    task_id='insert_data',
    dag=dag
)

create_database >> insert_data
