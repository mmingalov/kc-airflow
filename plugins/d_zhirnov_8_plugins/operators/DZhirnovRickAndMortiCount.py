import logging

import requests
import pandas as pd

from airflow import DAG, AirflowException
from airflow.models import BaseOperator


class DZhirnovRickAndMortiCountOperator(BaseOperator):
    """
       Находит заданное количество локаций сериала "Рик и Морти" с наибольшим количеством резидентов
    """
    template_fields = ('top_n',)
    ui_color = "#e0ffff"

    def __init__(self, top_n: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_n = top_n

    def execute(self, context, **kwargs):
        """
        Вычисление топ n
        """
        api_url ='https://rickandmortyapi.com/api/location'
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            data = r.json().get('results')
            logging.info(f'page_count = {len(data)}')
            residents_df = pd.DataFrame(data)
            logging.info(residents_df.head(3))
            residents_df['resident_cnt'] = residents_df.residents.apply(lambda x: len(x))
            residents_df.sort_values('resident_cnt', ascending=False, inplace=True)
            logging.info(residents_df.head(self.top_n))
            residents_df[['id', 'name', 'type', 'dimension', 'resident_cnt']]\
                .head(self.top_n)\
                .to_csv('resident_cnt', index=False, header=False)
            #kwargs['ti'].xcom_push(value=data, key='data')
            # return data
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')



