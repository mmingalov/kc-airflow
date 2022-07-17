"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-tselikov',
    'poke_interval': 600
}

with DAG("s-tselikov-test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-tselikov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )


    def print_args_func(arg1, arg2, **kwargs):
        logging.info('--------------------------------')
        logging.info(f'op_args, №1: {arg1}')
        logging.info(f'op_args, №2: {arg2}')
        logging.info('op_kwargs, №1: ' + kwargs['kwarg1'])
        logging.info('op_kwargs, №2: ' + kwargs['kwarg2'])
        logging.info('template_dict, gv_karpov: ' + kwargs['templates_dict']['gv_karpov'])
        logging.info('templates_dict, task_owner: ' + kwargs['templates_dict']['task_owner'])
        logging.info('context, {{ds}}: ' + kwargs['ds'])
        logging.info('context, {{tomorrow_ds}}: ' + kwargs['tomorrow_ds'])
        logging.info('--------------------------------')

    print_args = PythonOperator(
        task_id='print_args',
        python_callable=print_args_func,
        op_args=['arg1', 'arg2'],
        op_kwargs = {'kwarg1': 'kwarg1', 'kwarg2': 'kwarg2'},
        templates_dict = {'gv_karpov': '{{ var.value.gv_karpov }}',
                                        'task_owner': '{{ task.owner }}'},
        provide_context = True
    )


    dummy >> print_args >> [echo_ds, hello_world]