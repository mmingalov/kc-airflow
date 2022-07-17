import pandas as pd
import requests
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.exceptions import AirflowException

# pd.options.display.max_columns = 100


class OLikiyRAMTop3LocationOperator(BaseOperator):
    """
        Top-3 location for RAM API
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_url = 'https://rickandmortyapi.com/api/location'

    def get_loc_data(self, url):
        r = requests.get(url)
        if r.status_code == 200:
            result = r.json()['results']
            return result
        else:
            return 'Error in load result data'

    def top_3_location(self, url):
        res_dct = {'id': [], 'name': [], 'type': [], 'dimension': [], 'residents': []}
        for loc in self.get_loc_data(url):
            for key in loc.keys():
                if key in ['id', 'name', 'type', 'dimension']:
                    res_dct[key].append(loc[key])
                elif key == 'residents':
                    res_dct[key].append(len(loc[key]))
        return pd.DataFrame.from_dict(res_dct).sort_values(by=['residents'], ascending=False) \
            .rename(columns={'residents': 'resident_cnt'}).head(3)

    def execute(self, context):
        top3_df = self.top_3_location(self.api_url)
        destination = PostgresHook(postgres_conn_id='conn_greenplum_write')
        rows = [tuple(r) for r in top3_df.to_numpy().tolist()]
        fields = ['id', 'name', 'type', 'dimension', 'resident_cnt']
        destination.insert_rows(table='olikiy_ram_location', rows=rows, target_fields=fields)
