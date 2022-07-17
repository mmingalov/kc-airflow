import requests
import json
import pandas as pd
import logging
from airflow.models.baseoperator import BaseOperator
#######################################################################


class GritsenkoLocationOperator(BaseOperator):

    def __init__(self,  **kwargs) -> None:
        super().__init__(**kwargs)

    def load(self, context) -> pd.DataFrame:
        url = 'https://rickandmortyapi.com/api/location'
        resp = requests.get(url)
        if resp.status_code == 200:
            logging.info("SS")
            data = json.loads(resp.text)
            df = DataFrame()
            df2 = {}
            for value in data['results']:
                df2['id'] = value['id']
                df2['name'] = value['name']
                df2['type'] = value['type']
                df2['dimension'] = value['dimension']
                df2['resident_cnt'] = len(value['residents'])
                result = df.append([df2]).sort_values(
                    by='resident_cnt', ascending=False).head(3)
            return result
        else:
            logging.warning("Error {}".format(r.status_code))







