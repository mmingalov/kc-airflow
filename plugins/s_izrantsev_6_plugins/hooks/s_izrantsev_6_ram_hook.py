from airflow.providers.http.hooks.http import HttpHook


class IzrantsevRickAndMortyHook(HttpHook):
    """

    Hook для получения данных из API Rick and Morty
    get_loc_pages() для получения кол-ва страниц локаций
    get_results() для получения данных по локациям (id,name,type,dimension,resident_cnt)

    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_loc_pages(self):
        """ return number of api pages """
        return self.run('api/location').json()['info']['pages']

    def get_results(self, page_num: str) -> list:
        """ return results list of location api """
        return self.run(f'api/location?page={page_num}').json()['results']
