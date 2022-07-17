from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.decorators import task
from pendulum import datetime, duration
from airflow.providers.postgres.hooks.postgres import PostgresHook
from p_kovalev_8_plugins.PKovalevRamTopNLocations import PKovalevRamTopNLocations

DEFAULT_ARGS = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': duration(minutes=1),
    'email': 'pavelkov007@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='PKovalevLoadTopNRamLocations',
    schedule_interval=None,
    start_date=datetime(2022, 5, 15),
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=['PKovalev', 'p.kovalev', 'ram', 'locations', 'top 3'],
    render_template_as_native_obj=True

) as dag:

    @task
    def create_locations_table(**kwargs):

        pg_hook = PostgresHook(
            postgres_conn_id='conn_greenplum_write'
        )

        create_tbl_query = r'''
            
            CREATE TABLE IF NOT EXISTS public.pkovalev_ram_location
            (
                 id int not null PRIMARY KEY
                ,name text null
                ,type text null
                ,dimension text null
                ,resident_cnt int null
            )
            DISTRIBUTED BY (id)
            
        '''

        pg_hook.run(sql=create_tbl_query, autocommit=False)

    create_locations_table_task = create_locations_table()

    get_top_n_locations = PKovalevRamTopNLocations(
        task_id='get_top_n_locations',
        top_n="{{params.get('top_n')|int if params.get('top_n') else 3}}"
    )

    @task
    def load_top_n_locations(**kwargs):
        from contextlib import closing
        from psycopg2.extras import execute_values

        locations_data = kwargs['ti'].xcom_pull(task_ids='get_top_n_locations')

        pg_hook = PostgresHook(
            postgres_conn_id='conn_greenplum_write'
        )

        pg_hook.run(sql='TRUNCATE TABLE public.pkovalev_ram_location', autocommit=False)

        load_query = r'''
            INSERT INTO public.pkovalev_ram_location(id, name, type, dimension, resident_cnt)
            VALUES %s
        '''

        query_template = '(%(id)s, %(name)s, %(type)s, %(dimension)s, %(resident_cnt)s)'

        with closing(pg_hook.get_conn()) as conn:

            with closing(conn.cursor()) as cur:

                execute_values(cur, sql=load_query, argslist=locations_data, template=query_template)

            conn.commit()


    load_top_n_locations_task = load_top_n_locations()

    chain(create_locations_table_task, get_top_n_locations, load_top_n_locations_task)


