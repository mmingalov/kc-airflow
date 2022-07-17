import pandas as pd
import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class DavydovaLocationsWithMaxResidentsOperator(BaseOperator):
    """
    Finds top-3 locations with max residents count.
    """

    def __init__(self, url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url   # url to parse

    def get_pages_count(self) -> int:
        """
        Get count of pages in API
        :return: pages count
        """
        r = requests.get(self.url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json()['info']['pages']
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load pages count')

    def get_info_from_page(self, locations: list) -> pd.DataFrame:
        df = pd.DataFrame()
        for location in locations:
            df = df.append(
                {
                    'id': location['id'],
                    'name': location['name'],
                    'type': location['type'],
                    'dimension': location['dimension'],
                    'resident_cnt': len(location['residents']),
                }
                , ignore_index=True)
        return df

    def execute(self, context):
        """
        Gets top-3 locations with residents and saves to DB
        """
        df = pd.DataFrame()
        for page in range(self.get_pages_count()):
            r = requests.get(self.url + '?page=' + str(page + 1))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                df = df.append(self.get_info_from_page(r.json()['results']), ignore_index=True)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        df = df.sort_values(by='resident_cnt', ascending=False)[:3]
        logging.info(f'top-3 locations: {df}')
        context['ti'].xcom_push(value=df, key='davydova_ram')
