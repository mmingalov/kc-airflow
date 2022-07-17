from airflow.providers.http.hooks.http import HttpHook


class RAMAPILocationHook(HttpHook):

    def __init__(self, http_conn_id: str = ..., method: str = 'GET', **kwargs):
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)


    def __get_location_count(self):
        
        res = self.run('api/location?page=1')

        return res.json()['info']['count']

    
    def gen_location_schema(self):

        session = self.get_conn()
        for location_id in range(1, self.__get_location_count()+1):

            res = session.get(f'{self.base_url}/api/location/{location_id}')
            res.raise_for_status()

            yield res.json()
