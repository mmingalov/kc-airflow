from airflow import DAG
import logging
from textwrap import dedent

from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import macros

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-tskhay',
    'poke_interval': 600
}

with DAG("a-tskhay_templates",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    tags=['a-tskhay']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    template_str = dedent("""
    ______________________________________________________________
    ds: {{ ds }}
    week_day_num: {{ execution_date.weekday() }}
    ds_nodash: {{ ds_nodash }}
    ts: {{ts}}
    5 дней назад: {{ macros.ds_add(ds, -5) }}
    только год: {{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}
    unixtime: {{ "{:.0f}".format(macros.time.mktime(execution_date.timetuple())*1000) }}
    
    ______________________________________________________________
    """)

    def print_template_func(print_this):
        logging.info(print_this)

    print_templates = PythonOperator(
        task_id='print_templates',
        python_callable=print_template_func,
        op_args=[template_str]
    )

    dummy >> print_templates