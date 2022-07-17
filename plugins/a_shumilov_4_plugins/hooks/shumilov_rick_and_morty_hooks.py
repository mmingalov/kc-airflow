import logging
import json
from airflow.hooks.http_hook import HttpHook


class ShumilovRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def return_n_locations_max_residents(self, n: int, desc=True):
        """Returns last n locations with max residents in API"""
        json_res = self.run('api/location').json()['results']
        residents = {}
        for location in json_res:
            residents[len(location['residents'])] = location

        result = []
        for i in list(set(residents.keys()))[-1 * n:][::(-1 if desc else 1)]:
            temp = residents[i]
            temp['resident_cnt'] = i
            temp = {i: temp[i] for i in ['id', 'name', 'type', 'dimension', 'resident_cnt']}
            # result.append(temp)
            result.append(json.dumps(temp))

        return result

