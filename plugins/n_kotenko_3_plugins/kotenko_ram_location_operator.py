import requests
import pandas as pd
import logging

from airflow import AirflowException
from airflow.models import BaseOperator



class KotenkoRamLocationOperator(BaseOperator):
    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(api_url):
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_location_info(result_json):
        location_df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        for one_char in result_json:
            row = {
                'id': one_char.get('id'),
                'name': one_char.get('name'),
                'type': one_char.get('type'),
                'dimension': one_char.get('dimension'),
                'resident_cnt': len(one_char.get('residents'))
            }
            location_df = location_df.append(row, ignore_index=True)
        return location_df

    def execute(self, context):
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        result_df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                result_df = pd.concat([result_df, self.get_location_info(r.json().get('results'))])
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load location df from Rick&Morty API')

        result_df = result_df.reset_index().sort_values(by=['resident_cnt'], ascending=False).head(3)
        result_df.to_csv('kotenko_ram.csv', sep=';')
        logging.info(f'Succeed wrote location df: {len(result_df)}')

