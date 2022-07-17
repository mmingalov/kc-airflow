import logging

from airflow.hooks.http_hook import HttpHook


class SChertkovRickMortyApiHook(HttpHook):
    """
    Polling R&M API
    """
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def __get_locations_pages(self):
        """
        Lazily download pages from the locations API
        """
        logging.info("Getting a first page from the api")
        page = self.run('https://rickandmortyapi.com/api/location').json()
        yield page['results']
        while page['info']['next']:
            next_url = page['info']['next']
            logging.info(f"Getting next page, url: {next_url}")
            page = self.run(next_url).json()
            yield page['results']
        logging.info(f"Reached the end, it seems")

    def __get_locations(self):
        """
        Converts location to the desired format
        """
        for locations in self.__get_locations_pages():
            for loc in locations:
                location = {k: loc[k] for k in ['id', 'name', 'type', 'dimension']}
                location['resident_cnt'] = len(loc['residents'])
                yield location

    def get_locations(self) -> list:
        """
        Returns a list of locations with population size
        """
        return list(self.__get_locations())



