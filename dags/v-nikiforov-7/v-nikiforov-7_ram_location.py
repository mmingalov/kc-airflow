from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from v_nikiforov_7_ram_location.v_nikiforov_7_ram_location_plugin import TopLocationsOperator

my_id = 'v-nikiforov-7'

default_args = {
    'owner': my_id,
    'depends_on_past': False,
    'start_date': days_ago(1),
}


with DAG(
    dag_id=f'{my_id}_ram_location',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:

    start_flow = DummyOperator(task_id='start_flow')
    end_flow = DummyOperator(task_id='end_flow', trigger_rule='all_done')

    top_locations_to_gp = TopLocationsOperator(
        task_id='top_locations_to_gp',
    )

    start_flow >> top_locations_to_gp >> end_flow
