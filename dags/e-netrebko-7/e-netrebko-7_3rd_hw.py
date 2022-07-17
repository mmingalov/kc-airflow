from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from e_netrebko_7_plugins.e_netrebko_7_top_loc_operator import TopLocationsOperator

my_id = 'e-netrebko-7'

default_args = {
    'owner': my_id,
    'depends_on_past': False,
    'start_date': days_ago(1),
}


with DAG(
    dag_id=f'{my_id}_3rd_hw',
    default_args=default_args,
    schedule_interval='@once',
) as dag:

    start_flow = DummyOperator(task_id='start_flow')
    end_flow = DummyOperator(task_id='end_flow', trigger_rule='all_done')

    top_locations_to_gp = TopLocationsOperator(
        task_id='top_locations_to_gp',
    )

    start_flow >> top_locations_to_gp >> end_flow
