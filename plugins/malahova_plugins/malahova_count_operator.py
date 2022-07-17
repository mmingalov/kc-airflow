import logging
import pandas as pd
import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class InnaMalahovaCountOperator(BaseOperator):
    """
    Count number of residents
    """

    ui_color = "#e0ffff"

    def __init__(self, table_name: str, conn: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.conn = conn

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

    def execute(self, context):
        """
        Logging count of residents in Rick&Morty
        """
        data = pd.DataFrame()
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                result = r.json().get('results')
                df = pd.DataFrame(result)
                df['resident_cnt'] = df['residents'].apply(lambda x: int(len(x)))
                data = data.append(df)
                data = data.reset_index(drop=True)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        result_df = data.groupby(['id', 'name', 'type', 'dimension'])['resident_cnt'].sum()\
                        .reset_index()\
                        .sort_values('resident_cnt', ascending=False)\
                        .head(3)
        logging.info(f'Table with top-3 locations is recorded: {result_df.shape}')

        pg_hook = PostgresHook(postgres_conn_id=self.conn)
        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()
            cols = ','.join(list(result_df.columns))
            sql = f"""
                INSERT INTO {self.table_name}({cols}) VALUES(%s,%s,%s,%s,%s);
                """
            tpls = [tuple(x) for x in result_df.to_numpy()]
            logging.info(f'tuples with top-3 locations:\n{tpls}')
            cursor.executemany(sql, tpls)
            #conn.commit()
            select_all = f"""
                SELECT * FROM {self.table_name};
                """
            cursor.execute(select_all)
            result = cursor.fetchall()
            logging.info(f'recorded:\n{result}')