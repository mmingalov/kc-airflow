import requests
import logging
from operator import itemgetter

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class aromashkin_ram_top_tree_locations_Operator(BaseOperator):
    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        
# Функция для подсчета кол-ва страниц.
    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def top_tree_locations_func(self) -> list:
# Функция запроса локациий с максимальной популяцией.
        """
        Функция возвращает в XCom три локации с максимальной популяцией для послудующей загрузки в GreenPlum
        """
        location_list = []
        top_tree_locations = []
        location_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        
        for page in range(self.get_page_count(location_url.format(pg='1'))):
            r = requests.get(location_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                for foo in r.json()['results']:
                    #print(foo['id'], foo['name'], foo['type'], foo['dimension'], len(foo['residents']) ,end='\n') 
                    location_list.append([foo['id'], foo['name'], foo['type'], foo['dimension'], len(foo['residents'])])
                logging.info(f'Locations list: {location_list}')
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

            top_tree_locations = sorted(location_list, key=itemgetter(4))[-3:]
        logging.info(f'Top 3 locations: {top_tree_locations}')
        return top_tree_locations

    def execute(self, context):
        # 01. Подключаемся к Greenplum
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук

        insert_script = 'INSERT INTO public.a_romashkin_2_ram_location(id, name, type, dimension, resident_cnt)  VALUES '
        for foo in self.top_tree_locations_func():
            insert_script += "(" + str(foo[0]) + ", '" + foo[1] + "', '" + foo[2] + "', '" + foo[3] + "', " + str(foo[4]) + "),"
        insert_script = insert_script[:-1]    
        insert_script += ";"

        # 02. Вставляем значения в таблицу
        #insert_script = """ INSERT INTO public.a_romashkin_2_ram_location(id, name, type, dimension, resident_cnt) VALUES (1, 'Earth (C-137)', 'Planet', 'Dimension C-137', 27); """

        pg_hook.run(
        insert_script
        , False
        )