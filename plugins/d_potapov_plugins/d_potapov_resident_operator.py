import requests
import logging

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class dPotapovResidentsOperator(BaseOperator):
    ui_color = "#e0ffff"

    
    base_url = 'https://rickandmortyapi.com/api/location'
    page_url = 'https://rickandmortyapi.com/api/location?page={pg}'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self):
        r = requests.get(dPotapovResidentsOperator.base_url)

        if r.status_code == 200:
            page_count = r.json().get('info').get('pages')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_location_info_on_page(self,page_id):
        r = requests.get(dPotapovResidentsOperator.page_url.format(pg=page_id))

        if r.status_code == 200:

            result = r.json().get('results')
            location_info = []

            for i in result:
                id = i.get('id')
                name = i.get('name')
                type = i.get('type')
                dim = i.get('dimension')
                res_cnt = len(i.get('residents'))
                location_info.append((id,name,type,dim,res_cnt))

            return location_info
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        location_info = []
        pg_cnt = self.get_page_count()

        for page in range(1,pg_cnt+1,1):
            location_info.extend(self.get_location_info_on_page(page))

        result = sorted(location_info, key = lambda res: res[4],reverse=True)[:3]
        for res in result:
            SQL = f"""INSERT INTO d_potapov_ram_location
            SELECT {res[0]} as id, '{res[1]}' as name, '{res[2]}' as type, '{res[3]}' as dimension, {res[4]} as resident_cnt
            where NOT EXISTS(SELECT * from public.d_potapov_ram_location where id = {res[0]})"""
            pg_hook.run(SQL, False)