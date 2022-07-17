"""
    Select and display heading column's value based on the condition 
    that is ID in the selecting row equals to number of the day when DAG was running
    In the code uses decorators, PostgresHook and BranchOperator
"""
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context

import pendulum
import logging 

DEFAULT_ARGS = {
    'owner': 'j-chernigin-8',
    'start_date': pendulum.datetime(2022, 3, 1, tz='UTC'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='UTC'),
    'schedule_interval': '@daily'
}

@dag(
    max_active_runs=1,
    catchup=True,
    tags=['articles', 'greenplum', 'j-chernigin-8'],
    default_args=DEFAULT_ARGS
)
def select_and_log_articles():

    gp_connection = 'conn_greenplum'
    query = 'select heading from public.articles where id = $day_of_week$'

    @task
    def select_articles(query, gp_connection):
        context = get_current_context()
        ds = context['ds']

        ds_date = pendulum.parse(ds, exact=True)
        day_of_week_num = str(ds_date.day_of_week)
        day_of_week_name = ds_date.format('dddd')
        query = query.replace('$day_of_week$', day_of_week_num)

        logging.info('message="initial parameters", day_of_week_num="{day_of_week_num}", day_of_week_name="{day_of_week_name}", query="{query}"'.format(
            day_of_week_num=day_of_week_num,
            day_of_week_name=day_of_week_name,
            query=query
        ))

        pg_hook = PostgresHook(gp_connection)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

        if result is None:
            raise AirflowSkipException
        else:
            logging.info('Artcile\'s heading: {}'.format(result[0]))

    select_articles(query=query, gp_connection=gp_connection)


dag = select_and_log_articles()
