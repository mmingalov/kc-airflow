import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class ATskhayLocationOperator(BaseOperator):
    ui_color = "#96ff33"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, ram_api_url):
        r = requests.get(ram_api_url)
        if r.status_code == 200:
            ram_page_count = r.json()['info']['pages']
            logging.info('SUCCESS')
            logging.info(f'ram_page_count = {ram_page_count}')
            return ram_page_count
        else:
            logging.warning(f'HTTP STATUS {r.status_code}')
            raise AirflowException('Error in getting RaMs page count')

    def get_info_from_page(self, r_json):
        info_one_page = []
        for loc in r_json:
            info_one_page.append((loc['id'], loc['name'], loc['type'], loc['dimension'], len(loc['residents'])))
        return info_one_page

    def execute(self, context):
        ram_loc_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        info_all_pages = []
        page_count = self.get_page_count(ram_loc_url.format(pg=1))

        for page in range(1, page_count + 1):
            r = requests.get(ram_loc_url.format(pg=page))
            if r.status_code == 200:
                info_all_pages.extend(self.get_info_from_page(r.json()['results']))
            else:
                logging.warning(f'HTTP STATUS {r.status_code}')
                raise AirflowException('Error in getting RaMs location info')

        df = pd.DataFrame(data=info_all_pages, columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        df_sorted = df.sort_values(by='resident_cnt', ascending=False).reset_index(drop=True).head(3)

        return df_sorted
