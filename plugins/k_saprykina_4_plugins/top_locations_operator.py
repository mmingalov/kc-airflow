import pandas as pd

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class SaprRickMortyHook(HttpHook):
    """
    Get info from Rick and Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def location_pages_count(self):
        return self.run('api/location').json()['info']['pages']

    def locations(self, page_num: int) -> list:
        return self.run(f'api/location?page={page_num}').json()['results']


class TopLocations(BaseOperator):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        """
        Get top 3 locations from Rick and Morty API
        and save to Xcom
        """
        hook = SaprRickMortyHook('dina_ram')

        locations = []
        for idx in range(hook.location_pages_count()):
            one_page_locations = hook.locations(idx + 1)
            locations += one_page_locations

        for i in locations:
            i['residents'] = len(i['residents'])

        df = pd.DataFrame(locations)
        df.rename(columns={'residents': 'resident_cnt'}, inplace=True)
        df.drop(columns=['url', 'created'], inplace=True)
        df = df.sort_values(by='resident_cnt', ascending=False)[:3]

        context['ti'].xcom_push(value=df, key='ksaprykina_ram')
