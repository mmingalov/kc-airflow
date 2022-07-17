"""
Пример работы Taskflow API
"""

from airflow.utils.dates import days_ago
import logging

from airflow.decorators import task
from airflow.decorators import dag


DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'Karpov',
    'poke_interval': 600
}


@dag(default_args=DEFAULT_ARGS,
     schedule_interval='@daily',
     tags=['karpov'])
def dina_taskflow():

    @task
    def list_of_nums():
        return [1, 2, 3, 4, 6]

    @task
    def sum_nums(nums: list):
        return sum(nums)

    @task
    def print_sum(total: int):
        logging.info(str(total))

    print_sum(sum_nums(list_of_nums()))


dina_taskflow_dag = dina_taskflow()
