""" > Задание
1. Скачать репозиторий.
2. Настроить IDE для работы с ним.
3. Создать в папке dags папку по имени своего логина в Karpov LMS.
4. Создать в ней питоновский файл, начинающийся со своего логина.
5. Внутри создать даг из нескольких тасков, на своё усмотрение:
— DummyOperator
— BashOperator с выводом строки
— PythonOperator с выводом строки
— любая другая простая логика
6. Запушить даг в репозиторий.
7. Убедиться, что даг появился в интерфейсе airflow и отрабатывает без ошибок.

ПАМЯТКА (как запушить лабу в гит)
1. Отводите от мастера свою ветку
2. Пушите в неё изменения
3. Создаёте Merge Request на сайте, он становится красным
4. Ребейзите свою ветку на мастер (git rebase master && git push -f)
5. Merge Request становится зелёным и автоматически принимается, если всё хорошо"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import timedelta, datetime, date

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 's-fesenko-3',
    'depends_on_past': False,
    'email': ['fesenko91@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG("lesson_4",
          schedule_interval=timedelta(days=1),
          default_args=default_args,
          start_date=days_ago(7),
          max_active_runs=1,
          tags=['s-fesenko-3']
          ) as dag:

    begin = DummyOperator(task_id='begin')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ a-gajdabura }}',
        dag = dag
    )

    def log_ds_func(**kwargs):
        date = kwargs['templates_dict']['a-gajdabura']
        logging.info(date)

    log_ds = PythonOperator(
        task_id='log_ds',
        python_callable=log_ds_func,
        templates_dict= {'a-gajdabura':' {{ a-gajdabura }} '},
        dag=dag
    )
    def day_of_week():
        weekno = datetime.today().weekday()
        if weekno < 5:
            print ("Weekday")
        else:  # 5 Sat, 6 Sun
            print ("Weekend")

    weekday = PythonOperator(
        task_id='weekday',
        python_callable=day_of_week,
        dag = dag
    )

    end = DummyOperator(task_id='end') # test

    begin >> [echo_ds, log_ds] >> weekday >> end

    # Я просто пытаюсь разобраться с ГИТОМ, как все устроено, как пушатся ветки и тд. ОЧЕНЬ ИНТЕРЕСНО