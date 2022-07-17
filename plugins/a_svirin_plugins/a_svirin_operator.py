import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class SvirinRaMOperator(BaseOperator):
    """
    Find 3 locations with biggest resident count
    """

    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url):
        pg = 1
        r = requests.get(api_url.format(pg))
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info('page_count = {}'.format(page_count))
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_top_location(self, total_pages, ram_url, number_location):
        df_top_location = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])

        for page in range(total_pages):
            number_page = page + 1
            r = requests.get(ram_url.format(number_page))

            if r.status_code == 200:
                logging.info('PAGE {}'.format(number_page))

                for row in r.json()['results']:
                    data_location = {}
                    data_location['id'] = row.get('id')
                    data_location['name'] = row.get('name')
                    data_location['type'] = row.get('type')
                    data_location['dimension'] = row.get('dimension')
                    data_location['resident_cnt'] = len(row.get('residents'))
                    df_top_location = df_top_location.append(data_location, ignore_index=True)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        return df_top_location.sort_values('resident_cnt', ascending=False).head(number_location)

    def execute(self, context):
        ram_url = 'https://rickandmortyapi.com/api/location?page={}'
        number_location = 3

        total_pages = self.get_page_count(ram_url)

        result = self.get_top_location(total_pages, ram_url, number_location)
        return result
