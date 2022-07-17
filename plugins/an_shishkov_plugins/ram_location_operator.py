import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class anshishkovRamLocationsTopThreeOperator(BaseOperator):
    
    ui_color = "#e0ffff"
    
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

    def get_location_info(self, result_json):
        lst = list()
        for lctn in result_json:
            lst.append((lctn['id'], lctn['name'], lctn['type'], lctn['dimension'], len(lctn['residents'])))
        return lst

    def execute(self, context):
        ram_loc_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        lctns_lst = list()
        for page in range(1, self.get_page_count(ram_loc_url.format(pg=1)) + 1):
            rqst = requests.get(ram_loc_url.format(pg=page))
            if rqst.status_code == 200:
                logging.info(f'PAGE {page}')
                lctns_lst.extend(self.get_location_info(rqst.json()['results']))
            else:
                logging.warning(f'HTTP STATUS {r.status_code}')
                raise AirflowException('Error in getting RaMs location info')
        df = pd.DataFrame(data=lctns_lst, columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        df = df.sort_values(by='resident_cnt', ascending=False).reset_index(drop=True).iloc[0:3,:]
        return df
