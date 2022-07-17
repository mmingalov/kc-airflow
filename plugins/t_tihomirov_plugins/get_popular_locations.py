import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook

class TimRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_loc_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_loc_page(self) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location').json()['results']


class TimRamGatherData(BaseOperator):
    """
    On TimRickMortyHook
    """

    template_fields = ('get_max',)
    ui_color = "#c7ffe9"

    def __init__(self, get_max: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.get_max = get_max
        self.sql_create = """CREATE TABLE IF NOT EXISTS
                            t_tihomirov_ram_location(
                                id int,
                                name varchar(200),
                                type varchar(200),
                                dimension varchar(200),
                                resident_cnt int
                            )"""

    def create_table(self):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(self.sql_create, False)
        logging.info('Table created')

    def check_table(self):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("mycursor")
        sql = 'SELECT * from t_tihomirov_ram_location'
        cursor.execute(sql)
        data = cursor.fetchall()
        logging.info(str(data))
    
    def insert_to_greenplum(self, top_locations:list) -> None:
        insert_values = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                         for loc in top_locations]
        logging.info(insert_values)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        truncate_sql = '''TRUNCATE TABLE public."t_tihomirov_ram_location"'''
        pg_hook.run(truncate_sql, False)
        insert_sql = f'''INSERT INTO public."t_tihomirov_ram_location" VALUES {','.join(insert_values)}'''
        pg_hook.run(insert_sql, False)

    def execute(self, context) -> list:
        """
        Gathering and filtering data
        """
        hook = TimRickMortyHook('dina_ram')
        location_list = []
        one_page = hook.get_loc_page() #no need for pagination
        for location in one_page:
            location_dict = {
                'id': location['id'],
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            location_list.append(location_dict)
        
        sorted_locations = sorted(location_list,
                                  key=lambda cnt: cnt['resident_cnt'],
                                  reverse=True)
        top_locations = sorted_locations[:self.get_max]
        try:
            self.create_table()
        except Exception as e:
            logging.info(str(e))
        self.check_table()
        self.insert_to_greenplum(top_locations)
        self.check_table()
