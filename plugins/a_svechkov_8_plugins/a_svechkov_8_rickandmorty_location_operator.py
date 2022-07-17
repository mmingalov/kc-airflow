

import requests
import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class ASvechkov8RickAndMortyLocationOperator(BaseOperator):
    """
        находим топ-3 локации по количеству жителей
    """



    def __init__(self, table_name: str, conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.conn_id = conn_id

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_top3_on_page(self, result_json: list) -> int:
        """
        Get top3 locations in one page by quantity of residents.
        :param result_json:
        :return: pd.DataFrame with top-3
        """
        columns_lst = ['id', 'name', 'type', 'dimension', 'resident_cnt']
        top_df = pd.DataFrame(columns=columns_lst)
        line_series = pd.Series(index=columns_lst)
        for one_char in result_json:
            for column in columns_lst[:-1]:
                line_series[column] = one_char.get(column)
            line_series['resident_cnt'] = len(one_char.get('residents'))
            top_df = pd.concat([top_df, line_series.to_frame().T], ignore_index=True)
        top_df = top_df.sort_values('resident_cnt', ascending=False).head(3)
        return top_df

    def execute(self, context):
        """
        Find top-3 locations based on quantity of residents
        """
        columns_lst = ['id', 'name', 'type', 'dimension', 'resident_cnt']
        sum_df = pd.DataFrame(columns=columns_lst)
        ram_location_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_location_url.format(pg='1'))):
            r = requests.get(ram_location_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                sum_df = pd.concat([sum_df, self.get_top3_on_page(r.json().get('results'))],
                                   ignore_index=True)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        sum_df = sum_df.sort_values('resident_cnt', ascending=False)\
                        .head(3).reset_index(drop=True)
        insert_query = f"""
                                INSERT INTO {self.table_name} (id, name, type, dimension, resident_cnt)
                                VALUES (%s, %s, %s, %s, %s);
                                """
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sum_df.id.values.astype(int)
        sum_df.resident_cnt.values.astype(int)
        for i in range(3):
            position_string = tuple((sum_df.loc[i][0], sum_df.loc[i][1], sum_df.loc[i][2],
                                    sum_df.loc[i][3], sum_df.loc[i][4]))
            logging.info(f'location number {i}: {position_string}')
            cursor.execute(insert_query, position_string)
            conn.commit()

        cursor.execute(f'SELECT * FROM {self.table_name}')
        query_for_log = cursor.fetchall()
        logging.info('test query select_all')
        logging.info(query_for_log)
