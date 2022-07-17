import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


class RickMortyHook(HttpHook):

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def location_number(self):
        return self.run('api/location').json()['info']['count']

    def location_information(self, location_id):
        return self.run(f'api/location/{location_id}').json()


class RickMortyParse(BaseOperator):

    ui_color = "#a7eefa"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):

        hook = RickMortyHook(
            'dina_ram')
        values = []
        for location_id in range(hook.location_number()+1):
            location_info = hook.location_information(location_id)
            if 'error' in location_info:
                continue
            try:
                location_info['resident_cnt'] = len(location_info['residents'])
                values += [{a: location_info[a] for a in ['id', 'name',
                                                          'type', 'dimension', 'resident_cnt']}]
            except:
                continue
        key = [(i, d['resident_cnt']) for i, d in enumerate(values)]
        top_3 = sorted(key, key=lambda x: x[1])[-3:]
        ret = [values[i] for i, _ in top_3]
        logging.info(ret)
        return ret


CONNECTION = 'conn_greenplum_write'
TABLENAME = 'a_vdovenko_lesson_7'


def data_insertion(**kwargs):
    data = kwargs['ti'].xcom_pull(
        task_ids='rick_morty_parse', key='return_value')
    pg_hook = PostgresHook(postgres_conn_id=CONNECTION)

    for data_point in data:

        pg_hook.run(
            f'DELETE FROM {TABLENAME} WHERE id = {data_point["id"]};')

        pg_hook.run(
            f'INSERT INTO {TABLENAME} (id, name, type, dimension, resident_cnt) VALUES ( {data_point["id"]}, \'{data_point["name"]}\', \'{data_point["type"]}\', \'{data_point["dimension"]}\', {data_point["resident_cnt"]});')
