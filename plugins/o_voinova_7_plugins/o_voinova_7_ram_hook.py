import pandas as pd
from airflow.hooks.http_hook import HttpHook


class OVoinova7RickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = None
        self.base_url = 'https://rickandmortyapi.com'
        self.method = 'GET'

    def get_all_locations(self) -> pd.DataFrame:
        results = pd.DataFrame()
        for pg in range(self.get_location_page_count()):
            page_ = self.get_location_page(pg + 1)
            page_df = pd.DataFrame.from_records(page_)
            results = results.append(page_df)
        return results

    def get_location_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_location_page(self, page_num: int) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location/?page={page_num}').json()['results']
