"""
Домашнее задание - СЛОЖНЫЕ ПАЙПЛАЙНЫ, ЧАСТЬ 1
"""
from airflow import DAG
# from airflow.utils.dates import days_ago
import logging
# import datetime
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': '2022-03-02',
    'end_date': '2022-03-15',
    'owner': 'i.trubnikov',
    'poke_interval': 600
}


with DAG("i.trubnikov_complex_1",
    schedule_interval="0 0 * * 1-6",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i.trubnikov']
) as dag:
    from bs4 import BeautifulSoup
    import requests


    def corona_death():
        '''Парсинг единственного значения:
        количество умерших за последний день от коронавируса в РФ
        достаём значение "diedChange"
        '''
        url = 'https://xn--80aesfpebagmfblc0a.xn--p1ai/information/'
        page = requests.get(url)

        filteredNews = []
        allNews = []
        soup = BeautifulSoup(page.text, "html.parser")

        allNews = soup.findAll('cv-stats-virus')
        print(allNews)
        target_str = str(allNews)

        target_list = target_str.split(',')
        d = {}
        for i in target_list:
            b = i.replace('"', '').replace('+', '')
            split_list = b.split(':')
            d[split_list[0]] = split_list[1]
        died = int(d["diedChange"])
        txt = open('/tmp/died.txt','w')
        txt.write(str(died))
        txt.close()
        logging.info(f"СЕГОДНЯ умерло    {str(open('/tmp/died.txt','r'))}")

        return died


    def log():
        with open('/tmp/died.txt', 'r') as txt:
            logging.info(f"От коронавируса сегодня умерло:{txt.read()}")



    download_data_corona= PythonOperator(
    task_id='download_data_corona',
    python_callable=corona_death
    )



    log_died = PythonOperator(
        task_id= 'log_died',
        python_callable=log
    )


    def get_from_greenplum_func(article_id):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор



        # logging.info(f"СЕГОДНЯ     {exec_day}")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id}')  # исполняем sql
        query_res = cursor.fetchall()
        logging.info(f"СЕГОДНЯ     {article_id}")
        logging.info(f"данные из ГП   {query_res}")

    get_from_greenplum = PythonOperator(
    task_id= 'get_from_GP_and_log',
    python_callable=get_from_greenplum_func,
    op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ]
    )

    get_from_greenplum
    download_data_corona >> log_died
# download_data_corona >> log_died>>