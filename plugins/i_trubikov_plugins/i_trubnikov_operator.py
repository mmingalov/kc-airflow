import pandas as pd
import os
from airflow.models import BaseOperator

import requests
import logging



class i_trubnikov_LocCountOperator(BaseOperator):
    """
    Count number of dead concrete species
    """

    # template_fields = ('species_type',)
    ui_color = "#e0ff5f"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


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




    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        self.get_page_count("https://rickandmortyapi.com/api/location")
        all_locs = requests.get("https://rickandmortyapi.com/api/location")
        logging.info("Load page :1")
        page = 1
        for i in range(1, all_locs.json()['info']['pages'] + 1):
            logging.info("")  # создаём цикл с количеством страниц
            if i == 1:  # создаём первый датафрейм с помощью стартового АПИ
                df_full = pd.DataFrame(all_locs.json()['results'])
            else:
                data = requests.get(
                    f"https://rickandmortyapi.com/api/location?page={i}").json()
                page += 1
                logging.info(f"Load page: {page}")  # в цикле загружем остальные АПИ
                df_full = pd.concat([df_full, pd.DataFrame(data['results'])],
                                    ignore_index=True)
                logging.info("Page concat")

        df_result = df_full.loc[:, ['id', 'name', 'type', 'dimension']]  # создаём необходимую таблицу
        df_result['resident_cnt'] = df_full.loc[:, 'residents'].str.len()  # считаем количество резидентов на локациях
        df_result.sort_values(ascending=False, by=['resident_cnt'])  # сортируем по количеству резидентов
        result_table = df_result.sort_values(ascending=False, by=['resident_cnt'])
        result_table.set_index('id',inplace=True)
        logging.info("SUCCESS")
        logging.info(f"{result_table[0:3]}")

        os.makedirs('tmp/i.trubnikov/', exist_ok=True)
        result_table[0:3].to_csv('tmp/i.trubnikov/RAM_top_loc.csv')













