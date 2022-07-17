import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class BychkovLocationOperator(BaseOperator):
    
    template_fields = ('pages_count',)
    ui_color = "#e0ffff"

    def __init__(self, pages_count: int, gp_table_name: str, gp_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pages_count = pages_count
        self.gp_table_name = gp_table_name
        self.gp_conn_id = gp_conn_id

    def get_pages_count(self, api_url: str) -> int:
        
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info('HTTP Status: 200 OK')
            pages_count = r.json().get('info').get('pages')
            logging.info(f'Number of pages {pages_count}')
            return pages_count
        else:
            logging.warning(f'HTTP status code {r.status_code}')
            raise AirflowException('Error in load page count')
    

    def get_location(self, result_json: list) -> list:
        data = []
        for el in result_json:
            data.append(
                [el.get('id'), 
                el.get('name'), 
                el.get('type'), 
                el.get('dimension'), 
                len(el.get('residents'))])
        logging.info(f'Data is processed.')
        return data

    def df_to_list(self, df):  
        ls = []
        for i in range(self.pages_count):
            ls.append(tuple((df.iloc[i][0], df.iloc[i][1], df.iloc[i][2], df.iloc[i][3], df.iloc[i][4])))
        return ls


    def execute(self, context):
        
        url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        df_columns = ['id', 'name', 'type', 'dimension', 'resident_cnt']
        locations = pd.DataFrame(columns=df_columns)
        for page in range(self.get_pages_count(url.format(pg='1'))):
            r = requests.get(url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info('HTTP Status: 200 OK')
                locations_df_page = pd.DataFrame(self.get_location(r.json().get('results')), columns=df_columns).sort_values(by=['resident_cnt'], ascending=False).head(3)
                logging.info(f'Number of page: {page + 1}')
                locations = pd.concat([locations, locations_df_page], ignore_index=True)
            else:
                logging.warning(f'HTTP status code {r.status_code}')
                raise AirflowException('Error in load from Rick&Morty API')
        
        insert_query = f"""
                INSERT INTO {self.gp_table_name} (id, name, type, dimension, resident_cnt)
                VALUES (%s, %s, %s, %s, %s);
                """
 
        
        locations = locations.sort_values(by=['resident_cnt'], ascending=False).head(self.pages_count)
        locations = self.df_to_list(locations)
        pg_hook = PostgresHook(postgres_conn_id=self.gp_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {self.gp_table_name}')
        query_res = cursor.fetchall()
        cursor.executemany(insert_query, locations)
        cursor.execute(f'SELECT * FROM {self.gp_table_name}')
        query_res_after_insert = cursor.fetchall()
        conn.commit()