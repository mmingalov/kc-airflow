"""
Примеры XCom, Jinja и передачи параметров в PythonOperator
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'Karpov',
    'poke_interval': 600
}


with DAG("dina_examples",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['karpov']
         ) as dag:

    start = DummyOperator(task_id='start')

    # ---------------------------------------------------
    # op_args, op_kwargs, template_dicts, provide_context
    # ---------------------------------------------------

    def print_args_func(arg1, arg2, **kwargs):
        logging.info('------------------------------')
        logging.info(f'op_args, №1: {arg1}')
        logging.info(f'op_args, №2: {arg2}')
        logging.info('op_kwargs, №1: ' + kwargs['kwarg1'])
        logging.info('op_kwargs, №2: ' + kwargs['kwarg2'])
        logging.info('templates_dict, gv_karpov: ' + kwargs['templates_dict']['gv_karpov'])
        logging.info('templates_dict, task.owner: ' + kwargs['templates_dict']['task_owner'])
        logging.info('context, {{ a-gajdabura }}: ' + kwargs['a-gajdabura'])
        logging.info('context, {{ tomorrow_ds }}: ' + kwargs['tomorrow_ds'])
        logging.info('------------------------------')

    print_args = PythonOperator(
        task_id='print_args',
        python_callable=print_args_func,
        op_args=['arg1', 'arg2'],
        op_kwargs={'kwarg1': 'kwarg1', 'kwarg2': 'kwarg2'},
        templates_dict={'gv_karpov': '{{ var.value.gv_karpov }}',
                        'task_owner': '{{ task.owner }}'},
        provide_context=True
    )

    start >> print_args

    # ---------------------------------------------------
    # Jinja Templates
    # ---------------------------------------------------

    from textwrap import dedent

    template_str = dedent("""
    --------------------------------------------------------
    a-gajdabura: {{ a-gajdabura }}
    ds_nodash: {{ ds_nodash }}
    ts: {{ ts }}
    gv_karpov: {{ var.value.gv_karpov }}
    gv_karpov, course: {{ var.json.gv_karpov_json.course }}
    
    5 дней назад: {{ macros.ds_add(a-gajdabura, -5) }}
    только год: {{ macros.ds_format(a-gajdabura, "%Y-%m-%d", "%Y") }}
    unixtime: {{ "{:.0f}".format(macros.time.mktime(execution_date.timetuple())*1000) }}
    --------------------------------------------------------
    """)

    def print_template_func(print_this):
        logging.info(print_this)

    print_templates = PythonOperator(
        task_id='print_templates',
        python_callable=print_template_func,
        op_args=[template_str]
    )

    start >> print_templates

    # ---------------------------------------------------
    # XCom
    # ---------------------------------------------------

    def explicit_push_func(**kwargs):
        kwargs['ti'].xcom_push(value='Hello world!', key='hi')

    def implicit_push_func():
        return 'Some string from function'

    explicit_push = PythonOperator(
        task_id='explicit_push',
        python_callable=explicit_push_func,
        provide_context=True
    )

    implicit_push = PythonOperator(
        task_id='implicit_push',
        python_callable=implicit_push_func
    )

    def print_both_func(**kwargs):
        logging.info('--------------')
        logging.info(kwargs['ti'].xcom_pull(task_ids='explicit_push', key='hi'))
        logging.info(kwargs['templates_dict']['implicit'])
        logging.info('--------------')

    print_both = PythonOperator(
        task_id='print_both',
        python_callable=print_both_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="implicit_push") }}'},
        provide_context=True
    )

    start >> [explicit_push, implicit_push] >> print_both
