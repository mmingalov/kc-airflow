import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class RickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'
        
    def get_location_num(self):
        """Returns total number of location"""
        return self.run('api/location').json()['info']['count']
    
    def get_location_info(self, loc_id):
        """Returns info about location by ID"""
        return self.run(f'api/location/{loc_id}').json()



class RAM_TOP3_Operator(BaseOperator):
    """
    Get locations from R&M and select top-3 by resident_cnt
    """

    ui_color = "#a7eefa"
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        """
        Loads number of locations, and fetches information about every
        """
        hook = RickMortyHook('dina_ram') # I hope, this connection is valid, because i'm not an admin to create my own.
        data = []
        for loc_id in range(hook.get_location_num()+1):
            loc_info = hook.get_location_info(loc_id)
            if 'error' in loc_info:
                continue #Smt is wrong!
            try:
                loc_info['resident_cnt'] = len(loc_info['residents'])
                data += [{a:loc_info[a] for a in ['id', 'name', 'type', 'dimension', 'resident_cnt']}]
            except:
                continue
        rc_key = [(i, d['resident_cnt']) for i, d in enumerate(data)]
        top3 = sorted(rc_key, key = lambda x:x[1])[-3:]
        ret = [data[i] for i, _ in top3]
        logging.info(ret)
        return ret #Everything in XCOM"