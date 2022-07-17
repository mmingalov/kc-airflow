"""
Operator for Rick and Morty API
"""

import logging
import requests
import pandas as pd
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class JTsyrlinLocationOperator(BaseOperator):
    """
    Get page number in API and download all location description
    """
    ui_color = "#e0ffff"

    def __init__(self, gp_table_name: str, gp_conn: str, top_n: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.gp_table_name = gp_table_name
        self.gp_conn = gp_conn
        self.top_n = top_n

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: total_page_num
        """
        res = requests.get(api_url)
        if res.status_code == 200:
            logging.info("SUCCESS")
            total_page_num = res.json().get('info').get('pages')
            logging.info(f'Pages successfully received. Total number: {total_page_num}')
            return int(total_page_num)
        else:
            logging.warning("HTTP STATUS {}".format(res.status_code))
            raise AirflowException('Error in load page count')

    def upload_data_to_greenplum(self, top_location_df: pd.DataFrame()):
        pg_hook = PostgresHook(postgres_conn_id=self.gp_conn)
        pg_conn = pg_hook.get_conn()

        try:
            with pg_conn.cursor() as cur:
                for i, row in top_location_df.iterrows():
                    name_value, type_value = "'" + row['name'] + "'", "'" + row['type'] + "'"
                    dim_value = "'" + row['dimension'] + "'"
                    insert_single_location_query = f'''
                                                insert into public.j_tsyrlin_3_top_locations
                                                (id, name, type, dimension, resident_cnt)
                                                values (
                                                        {row['id']},
                                                        {name_value},
                                                        {type_value},
                                                        {dim_value},
                                                        {row['resident_cnt']}
                                                       );       
                                                    '''
                    logging.info(insert_single_location_query)
                    cur.execute(insert_single_location_query)
                    logging.info(f'Top {i} location inserted.')
                    pg_conn.commit()
            pg_conn.close()

        except Exception as e:
            logging.info(f'Table Upload failed due to: {e}')
            raise AirflowException('Error in loading greenplum table.')

    def execute(self, context):
        """
        Parse all pages and count resident num
        """
        api_url = 'https://rickandmortyapi.com/api/location'

        for i in range(1, self.get_page_count(api_url)):
            res = requests.get(f'{api_url}?page={i}')
            if i == 1:
                df = pd.DataFrame(res.json().get('results'))
                df['resident_cnt'] = df['residents'].apply(lambda x: len(set(x)))
                df = df[['id', 'name', 'type', 'dimension', 'resident_cnt']]
            else:
                page_location = pd.DataFrame(res.json().get('results'))
                page_location['resident_cnt'] = page_location['residents'].apply(lambda x: len(set(x)))
                page_location = page_location[['id', 'name', 'type', 'dimension', 'resident_cnt']]
                df = df.append(page_location)
        df = df.drop_duplicates(['name', 'type']).sort_values('resident_cnt', ascending=False)
        logging.info(f'Locations found: {df.shape[0]}')
        logging.info('Table head:\n')
        logging.info(f'{df.head(3)}')
        self.upload_data_to_greenplum(df.iloc[:self.top_n, :])
        logging.info('JTsyrlin Operator work completed.')
