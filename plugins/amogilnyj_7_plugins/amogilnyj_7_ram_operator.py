import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
###############
class Top3LocationOperator(BaseOperator):

    ui_color = "#e0ffff"

    def __init__(self, *args, **kwargs) -> None:
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
        api_url = 'https://rickandmortyapi.com/api/location?page={pg}'

        result_df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])

        for page in range(self.get_page_count(api_url.format(pg='1'))):
            r = requests.get(api_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                page_result = r.json().get('results')
                result_df_len = len(
                    result_df)
                for i in range(len(page_result)):
                    result_df.loc[result_df_len + i] = [page_result[i]['id'], page_result[i]['name'],
                                                        page_result[i]['type'],
                                                        page_result[i]['dimension'], len(page_result[i]['residents'])]
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        result_top_3_df = result_df.sort_values(by='resident_cnt', ascending=False, ignore_index=True).head(3)
        result_top_3_dict = result_top_3_df.to_dict()
        return result_top_3_dict
