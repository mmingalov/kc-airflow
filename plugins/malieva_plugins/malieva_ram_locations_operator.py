import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class MAlievaLocationOperator(BaseOperator):
    """
    LOAD THE MOST POPULATED LOCATIONS INTO GP
    """

    template_fields = ('no_of_loc',)
    ui_color = "#e0ffff"

    def __init__(self, no_of_loc: int, gp_table_name: str, gp_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.no_of_loc = no_of_loc
        self.gp_table_name = gp_table_name
        self.gp_conn_id = gp_conn_id

    def get_no_of_pages(self, api_url: str) -> int:
        """
        Return number of pages to parse in API
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info('HTTP Status: 200 OK')
            no_of_pages = r.json().get('info').get('pages')
            logging.info(f'Number of pages {no_of_pages}')
            return no_of_pages
        else:
            logging.warning(f'HTTP status code {r.status_code}')
            raise AirflowException('Error has occured while getting  number of pages')
    

    def get_loc_info_on_page(self, result_json: list) -> list:
        """
        GET DATA IN ONE PAGE OF LOCATIONS
        """
        one_page_info = []
        for one_loc in result_json:
            one_page_info.append(
                [one_loc.get('id'), 
                one_loc.get('name'), 
                one_loc.get('type'), 
                one_loc.get('dimension'), 
                len(one_loc.get('residents'))])
        logging.info(f'Page is processed.')
        return one_page_info

    def df_to_list_of_tuples(self, df):  
        loc = []
        for i in range(self.no_of_loc):
            loc.append(tuple((df.iloc[i][0], df.iloc[i][1], df.iloc[i][2], df.iloc[i][3], df.iloc[i][4])))
        logging.info(f'List of tuples: {loc}')
        return loc

    def execute(self, context):
        """
        Load locations into GP
        """
        locations_url = 'https://rickandmortyapi.com/api/location/?page={no_of_page}'
        df_col_names = ['id', 'name', 'type', 'dimension', 'resident_cnt']
        locations_df = pd.DataFrame(columns=df_col_names)
        for page in range(self.get_no_of_pages(locations_url.format(no_of_page='1'))):
            r_page = requests.get(locations_url.format(no_of_page=str(page + 1)))
            if r_page.status_code == 200:
                logging.info('HTTP Status: 200 OK')
                locations_df_page = pd.DataFrame(self.get_loc_info_on_page(r_page.json().get('results')), columns=df_col_names).sort_values(by=['resident_cnt'], ascending=False).head(3)
                logging.info(f'Number of page: {page + 1}')
                locations_df = pd.concat([locations_df, locations_df_page], ignore_index=True)
            else:
                logging.warning(f'HTTP status code {r_page.status_code}')
                raise AirflowException('Error in load from Rick&Morty API')
        
        locations_df = locations_df.sort_values(by=['resident_cnt'], ascending=False).head(self.no_of_loc)

        pg_hook = PostgresHook(postgres_conn_id=self.gp_conn_id)
        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()
            # loc_records = ", ".join(["%s"] * self.no_of_loc)
            insert_query = f"""
                INSERT INTO {self.gp_table_name} (id, name, type, dimension, resident_cnt)
                VALUES (%s, %s, %s, %s, %s);
                """
            locations = self.df_to_list_of_tuples(locations_df)
            cursor.execute(f'SELECT * FROM {self.gp_table_name}')
            query_res = cursor.fetchall()
            logging.info(f'OUTPUT before:\n{query_res}')
            cursor.executemany(insert_query, locations)
            cursor.execute(f'SELECT * FROM {self.gp_table_name}')
            query_res_after_insert = cursor.fetchall()
            conn.commit()
            logging.info(f'OUTPUT after:\n{query_res_after_insert}')


