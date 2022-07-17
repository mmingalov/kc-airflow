"""
Hook for Locations interaction over HTTP API
"""

from airflow.providers.http.hooks.http import HttpHook


class RAMLocationHook(HttpHook):
    _LOCATION = 'api/location'

    def __init__(self, http_conn_id, *args, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, *args, **kwargs)
        self.method = 'GET'

    def get_pages_count(self):
        return self.run(self._LOCATION).json()['info']['pages']

    def get_page_results(self, page_num):
        return self.run(f'{self._LOCATION}?page={page_num}').json()['results']
