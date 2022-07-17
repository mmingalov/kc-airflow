import requests
import logging
import pandas as pd
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class TopLocationsOperator(BaseOperator):
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
    
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
        api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        locations = {'id':[], 'name':[], 'type':[], 'dimension':[], 'resident_cnt': []}
        for page in range(self.get_page_count(api_url.format(pg='1'))):
            r = requests.get(api_url.format(pg=str(page + 1))).json().get('results')
            for i in r:
                locations['id'].append(i['id'])
                locations['name'].append(i['name'])
                locations['type'].append(i['type'])
                locations['dimension'].append(i['dimension'])
                locations['resident_cnt'].append(len(i['residents']))
        df = pd.DataFrame.from_dict(locations)
        df = df.sort_values('resident_cnt', ignore_index=True, ascending=False).head(3)
        tuples = [tuple(x) for x in df.to_numpy()]
        return ','.join(map(str, tuples))
