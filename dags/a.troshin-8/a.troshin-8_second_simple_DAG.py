"""
Second simple DAG. First DAG with additional func from Lesson 4
"""
from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a.troshin-8',
    'poke_interval': 600
}

with DAG(
        "a.troshin-8_DAG_01_04_part_2",
        schedule_interval='15 12 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['a.troshin-8']
        ) as dag:

    april_fools_day_today = BashOperator(
        task_id='april_fools_day_today',
        bash_command='echo "today {{ ds }}  Happy April Fool s Day"',
        dag=dag
    )

    def april_fools_day_left_func(**kwargs):
        str_ = ''' Today {cur_date}. April Fool's Day was {delta} days ago :('''
        cur_date = datetime.strptime(kwargs.get('ds'), '%Y-%m-%d')
        delta = (cur_date - datetime(2022, 4, 1)).days
        logging.info(str_.format(cur_date=cur_date.date(), delta=delta))

    april_fools_day_left = PythonOperator(
        task_id='april_fools_day_left',
        python_callable=april_fools_day_left_func,
        dag=dag
    )

    date_condition = BranchDateTimeOperator(
        task_id='date_condition',
        follow_task_ids_if_true=['april_fools_day_today'],
        follow_task_ids_if_false=['april_fools_day_left'],
        target_lower=datetime(2022, 4, 1, 0, 0, 0),
        target_upper=datetime(2022, 4, 1, 23, 59, 59),
        dag=dag
    )

    congratulations_ok = DummyOperator( 
        task_id='congratulations_ok', 
        trigger_rule='one_success', 
        dag=dag 
    )

    def detecting_day_of_week_func(**kwargs):
        today = kwargs.get('ds')
        kwargs['ti'].xcom_push(value=datetime.strptime(today, "%Y-%m-%d").isoweekday(), key='dow')
    
    detecting_day_of_week = PythonOperator(
        task_id='detecting_day_of_week',
        python_callable=detecting_day_of_week_func,
        provide_context=True,
        trigger_rule='one_success'
    )

    def query_heading_with_day_of_week_id_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        today_day_of_week = kwargs['ti'].xcom_pull(task_ids='detecting_day_of_week', key = 'dow')
        logging.info('Today {n} day of week, so relative heading is:'.format(n=today_day_of_week))
        request_to_base = 'SELECT heading FROM public.articles WHERE id = ' + str(today_day_of_week)
        cursor.execute(request_to_base)
        logging.info(cursor.fetchone()[0])

    query_heading_with_day_of_week_id = PythonOperator(
        task_id='query_heading_with_day_of_week_id',
        python_callable=query_heading_with_day_of_week_id_func,
        trigger_rule='one_success',
        provide_context=True,
        dag=dag
    )

    dag.doc_md = __doc__ 
    date_condition.doc_md = """ Определяет, является ли сегодняшняя дата первым апреля 2022г."""
    april_fools_day_today.doc_md = """ Печатает текущую дату и поздравление с Днем Дурака """
    april_fools_day_left.doc_md = """ Выводит в лог текущую дату и количество дней, 
                                      прошедших с первого апреля"""
    congratulations_ok.doc_md = """ Просто Dummy оператор"""
    detecting_day_of_week.doc_md = """ Определяет день недели по iso и передает в XCom"""
    query_heading_with_day_of_week_id.doc_md = """
            Забирает из XCom номер дня недели, подключается к базе GreenPlum, передает запрос для 
            выбора заголовка из базы public.articles, id которого равен дню недели по iso, 
            затем выводит данный заголовк в лог"""

    date_condition >> [april_fools_day_today, april_fools_day_left]\
        >> congratulations_ok >> detecting_day_of_week >> query_heading_with_day_of_week_id
