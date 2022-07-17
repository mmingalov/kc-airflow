from airflow.providers.http.hooks.http import HttpHook


class RickAndMortyLocationHook(HttpHook):
    """Hook for get location info from
    https://rickandmortyapi.com/documentation/#location-schema.

    Args:
        HttpHook (class): Interact with HTTP servers.
    """

    def __init__(self, http_conn_id: str = ..., method: str = 'GET', **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)


    def __get_location_count(self) -> int:
        """Get count all location.

        Returns:
            int: Location count.
        """
        
        r = self.run('api/location?page=1')

        return r.json()['info']['count']

    
    def gen_location_schema(self) -> dict:
        """Get single location schema for specified id.

        Returns:
            dict: Location schema.
        """

        session = self.get_conn()
        for location_id in range(1, self.__get_location_count()+1):

            r = session.get(f'{self.base_url}/api/location/{location_id}')
            r.raise_for_status()

            yield r.json()