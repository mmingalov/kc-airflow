"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator

import vk_api

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'd.nigmatullin-9'
    # 'poke_interval': 600
}

with DAG("d.nigmatullin-9_pochti",
    schedule_interval='@hourly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d.nigmatullin-9_pochti']
) as dag:

    def send_first_massage():
        token = '278b1233603b094b886891df17401f8058e586592a561dfcb7f5ccf102aa9dc035b3d8aa8ef2d1c1e8899'
        vk_session = vk_api.VkApi(token=token)
        vk = vk_session.get_api()
        vk.messages.send(
            chat_id=1,
            random_id=2,
            message='odin')

    def send_second_massage():
        token='278b1233603b094b886891df17401f8058e586592a561dfcb7f5ccf102aa9dc035b3d8aa8ef2d1c1e8899'
        vk_session = vk_api.VkApi(token=token)
        vk = vk_session.get_api()
        vk.messages.send(
            chat_id=1,
            random_id=2,
            message='d.va')


    first_massage = PythonOperator(
        task_id='first_massage',
        python_callable=send_first_massage,
        dag=dag
    )

    second_massage = PythonOperator(
        task_id='second_massage',
        python_callable=send_second_massage,
        dag=dag
    )

    first_massage >> second_massage