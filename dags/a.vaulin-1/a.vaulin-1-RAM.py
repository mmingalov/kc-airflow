from vaulin_plugins.vaulin_ram_lactions_info_operator import VaulinRamLocationInfoOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


TABLE_NAME = 'vaulin_ram_top_location'
GP_CONNECTION = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'Karpov',
    'poke_interval': 600,
    'catchup': False,
}


with DAG("a.vaulin-1-RAM",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov', 'vaulin']
         ) as dag:

    def write_top3_locations(**kwargs):

        locations = kwargs['ti'].xcom_pull(task_ids='get_locations_info')

        d = {x[0]: x[4] for x in locations}
        top3_id = sorted(d, key=d.get, reverse=True)[:3]
        top3_loc = [locations[x-1] for x in top3_id]

        pg_hook = PostgresHook(GP_CONNECTION)

        values = [
            f'''
                (
                    {row[0]},
                    '{row[1].replace("'", '"') }',
                    '{row[2].replace("'", '"') }',
                    '{row[3].replace("'", '"') }',
                    {row[4]}
                )
            '''
            for row in top3_loc
        ]

        sql = f'''
                 TRUNCATE {TABLE_NAME};
                 INSERT INTO {TABLE_NAME} (
                    id,
                    name,
                    type,
                    dimension,
                    resident_cnt
                 ) VALUES {','.join(values)};
        '''

        pg_hook.run(sql)

        return len(top3_loc)

    find_top3_locations = PythonOperator(
        task_id='write_top3_locations',
        python_callable=write_top3_locations,
        provide_context=True
    )

    get_locations_info = VaulinRamLocationInfoOperator(
        task_id='get_locations_info',
        do_xcom_push=True
    )

    create_table = PostgresOperator(
        task_id='create_table',
        sql=f'''
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER NOT NULL,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INTEGER NOT NULL,
                PRIMARY KEY (id)
                );
            ''',
        postgres_conn_id=GP_CONNECTION
    )

    create_table >> get_locations_info >> find_top3_locations
