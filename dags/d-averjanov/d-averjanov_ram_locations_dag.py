from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from d_averjanov_plugins.top_locations_operator import TopLocationsOperator



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-averjanov',
    'poke_interval': 600,
    'retries': 2,
    'schedule_interval' : '@once',
    'catchup':False
    }


api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'

with DAG('d-averjanov_ram_locations',
         default_args = DEFAULT_ARGS,
         tags = ['d-averjanov'],
         ) as dag:

    start = DummyOperator(task_id = 'start', dag = dag)

    end = DummyOperator(task_id = 'end', dag = dag)

    top_3_location = TopLocationsOperator(task_id = 'top_3_location')
    
    load_data_to_gp = PostgresOperator(task_id = 'load_data_to_gp',
                                       postgres_conn_id = 'conn_greenplum_write',
                                       sql = ['TRUNCATE TABLE "d-averjanov_ram_location"',
                                             'INSERT INTO "d-averjanov_ram_location"'
                                             'VALUES {{ ti.xcom_pull(task_ids = "top_3_location") }}']
                                             )

    start >> top_3_location >> load_data_to_gp >> end
