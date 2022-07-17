import logging
import requests
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context


class TopLocationsGrachevaOperator(BaseOperator):

    ui_color = "#A7A1D8"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def get_page_count(self, api_url: str) -> int:
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
        api_url = 'https://rickandmortyapi.com/api/location?page={page}'
        locations = []
        for page in range(self.get_page_count(api_url.format(page='1'))):
            r = requests.get(api_url.format(page=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                page_result = r.json().get('results')
                for i in range(len(page_result)):
                    locations.append([page_result[i]['id'], page_result[i]['name'],
                                      page_result[i]['type'], page_result[i]['dimension'],
                                      len(page_result[i]['residents'])])
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load page count')
        top_locations = sorted(locations, key=lambda x: x[4], reverse=True)[:3]
        logging.info(top_locations)
        return top_locations

