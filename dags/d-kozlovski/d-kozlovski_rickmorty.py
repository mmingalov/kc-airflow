import logging
from datetime import datetime

from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 5, 9),
    'owner': 'd-kozlovski',
    'poke_interval': 600,
    'trigger_rule':  'all_success'
}
#-----------определяем даг----------------#
with DAG('d-kozlovski-rickandmorty',
         schedule_interval="@once",
         default_args=DEFAULT_ARGS,
         max_active_runs = 1,
         tags=['d-kozlovski']
    ) as dag:

    #extract
    def extract_rickmorty_loc_stat_func(**kwargs):
        import requests
        results_location_stat = []
        api_url = "https://rickandmortyapi.com/api/location"
        r = requests.get(api_url).json()
        pages = r['info']['pages']

        for i in range(pages):
            api_url_page = api_url+"?page={}".format(i+1)
            r = requests.get(api_url_page).json()
            loc_list = r['results']

            for location in loc_list:
                id = location['id']
                name = location['name']
                type = location['type']
                dimension = location['dimension']
                resident_cnt = len(location['residents'])
                results_location_stat.append((id, name, type, dimension, resident_cnt))

        top3 = sorted(results_location_stat, key = lambda x: x[4], reverse=True)[:3]
        print('top3', top3)
        kwargs['ti'].xcom_push(value = top3, key='top3_location')
        # get_location_list()
        #
        # l = [(20, 'Earth (Replacement Dimension)', 'Planet', 'Replacement Dimension', 230),
        #     (3, 'Citadel of Ricks', 'Space station', 'unknown', 101),
        #     (6, 'Interdimensional Cable', 'TV', 'unknown', 62)]

            # string = ("insert into table values " + " ".join(["{}" for i in l])).format(*l)
            # print(string)
    extract_rickmorty_loc_stat = PythonOperator(
            task_id = 'extract_rickmorty_loc_stat',
            python_callable = extract_rickmorty_loc_stat_func
        )

    create_ram_location_table = PostgresOperator(
            task_id = 'create_ram_location_table',
            postgres_conn_id = "conn_greenplum_write",
            sql = """
                create table if not exists "d-kozlovski_ram_location" (
                     id    			integer unique  ,
                     name       	character(255)  ,
                     type    		character(255)  ,
                     dimension  	character(255)  ,
                     resident_cnt   integer
                    );"""
        )

    def load_top_3_func(**kwargs):
        #get  xcom
        top3 = kwargs['ti'].xcom_pull(task_ids='extract_rickmorty_loc_stat', key='top3_location')

        #query
        # string = ("insert into table values " + ", ".join(["{}" for i in top3])).format(*top3)
        # string += " ON CONFLICT (id) DO UPDATE"
        # query = "insert into  heading FROM articles WHERE id = {weekday}".format(weekday = str(weekday))

        #get conn
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор

        for row in top3:
            id = row[0]
            #проверим существет ли уже такая запись
            query_exist = """SELECT * FROM public."d-kozlovski_ram_location" where id = {}""".format(id)
            cursor.execute(query_exist)
            query_res = cursor.fetchall()
            is_row_already_exist = query_res != [()]
            print('query_res', query_res)
            print('is_row_already_exist', str(is_row_already_exist))
            if is_row_already_exist: #если такая запись уже существует, то ее нужно удалить чтобы потом обновить
                #dlelete
                query_delete = """DELETE FROM public."d-kozlovski_ram_location" where id = {} """.format(id)
                cursor.execute(query_delete)
            #вставить значение
            query_insert = """INSERT INTO public."d-kozlovski_ram_location" values {}""".format(row)
            cursor.execute(query_insert)
        conn.commit()
        conn.close()

    load_top_3 = PythonOperator(
                task_id = 'load_top_3',
                python_callable = load_top_3_func
            )


    [extract_rickmorty_loc_stat , create_ram_location_table] >> load_top_3
