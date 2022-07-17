"""First for kc DAG with DummyOperator, BashOperator, BranchPythonOperator"""

import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.dates import days_ago


query_gp_articles = '''SELECT heading FROM articles WHERE id = {}'''

default_args = {
    'start_date': datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14),
    'owner': 'o-likiy',
    'poke_interval': 600
}


def print_from_python(text):
    print(text)


def branch_selection(**kwarg):
    if kwarg['execution_date'].weekday() in [0, 1, 2, 3, 4, 5]:
        return 'workdays_and_saturday_task'
    else:
        return 'sunday_task'


def branch_workdays_and_saturday(query_src, **kwarg):
    src = PostgresHook(postgres_conn_id='conn_greenplum')
    df_src = src.get_pandas_df(query_src.format(kwarg['execution_date'].weekday() + 1))
    kwarg['ti'].xcom_push(value=df_src.values[0][0], key=kwarg['execution_date'].date().strftime('%Y-%m-%d'))


def branch_sunday(day_of_the_week, **kwarg):
    print(kwarg['execution_date'],
          day_of_the_week[kwarg['execution_date'].weekday()], 'Query is Not Working on Sundays!!!')


with DAG(
    dag_id='o-likiy_first_dag_modify',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    tags=['o-likiy'],
) as dag:

    start_task = DummyOperator(task_id='start_task')

    bash_task = BashOperator(task_id='bash_task',
                             bash_command="echo 'Hello World from BashHOperator!'")

    python_task = PythonOperator(task_id='python_task',
                                 python_callable=print_from_python,
                                 op_kwargs={'text': 'Hello World from PythonOperator!'})

    select_branch_task = BranchPythonOperator(task_id='select_branch_task',
                                              python_callable=branch_selection)

    workdays_and_saturday_task = PythonOperator(task_id='workdays_and_saturday_task',
                                                python_callable=branch_workdays_and_saturday,
                                                op_kwargs={'query_src': query_gp_articles},
                                                provide_context=True)

    sunday_task = PythonOperator(task_id='sunday_task',
                                 python_callable=branch_sunday,
                                 op_kwargs={'day_of_the_week': ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
                                                                'Friday', 'Saturday', 'Sunday']}, )

    end_task = DummyOperator(task_id='end_task',
                             trigger_rule=TriggerRule.NONE_FAILED)

    start_task >> bash_task >> python_task >> select_branch_task >> [workdays_and_saturday_task, sunday_task] >> end_task
