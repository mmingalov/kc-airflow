import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class BagadaevaRaMOperator(BaseOperator):
    """
    Find 3 locations with biggest resident count
    """

    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_data(self, result_json: list):
        dataframe = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        for row in result_json:
            dta = {}
            dta['id'] = row.get('id')
            dta['name'] = row.get('name')
            dta['type'] = row.get('type')
            dta['dimension'] = row.get('dimension')
            dta['resident_cnt'] = len(row.get('residents'))
            print(dta)
            dataframe = dataframe.append(dta, ignore_index=True)
        return dataframe

    def get_3_max(self, dataframe):
        res = dataframe.sort_values('resident_cnt', ascending=False).head(3)
        return res

    def get_page_count(self, api_url):
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
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context):
        """
        Find 3 locations with max resident count
        """
        dataframe = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                dataframe = dataframe.append(self.get_data(r.json().get('results')), ignore_index=True)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        result = self.get_3_max(dataframe)
        return result

