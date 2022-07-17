import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


class YBotnikov_RickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_all_locations(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['results']


class Top3Locations(BaseOperator):
    """
    Returns info on TOP3 Locations
    """

    ui_color = "#face8d"

    def __init__(self, top: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top = top

    def execute(self, context):
        """
        Getting TOP locations
        """
        hook = YBotnikov_RickMortyHook('dina_ram')
        loc_dict = hook.get_all_locations()
        top = sorted(loc_dict, key=lambda d: len(d['residents']), reverse=True)[:self.top]
        for row in top:
            row['residents'] = len(row['residents'])
        logging.info(f'TOP{self.top} locations in Rick&Morty \n{top}')

        pg_hook = PostgresHook('conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
        Create table if not exists y_botnikov_ram_location (
            id int,
            name text,
            type text,
            dimension text,
            resident_cnt bigint)
        ''')
        cursor.execute('Truncate table y_botnikov_ram_location')
        for row in top:
            cursor.execute(f'''
            insert into y_botnikov_ram_location(id, name, type, dimension, resident_cnt)
            values ({row['id']}, '{row['name']}', '{row['type']}', '{row['dimension']}', {row['residents']})
            ''')
        conn.commit()