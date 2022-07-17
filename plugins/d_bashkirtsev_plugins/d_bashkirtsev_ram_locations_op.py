import logging
from airflow.models import BaseOperator
import requests
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook


class d_bashkirtsev_ram_locations_op(BaseOperator):
    ui_color = "#e0ffff"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def get_page_count(self, ram_loc_url: str) -> int:
        r = requests.get(ram_loc_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, **kwargs):
        ram_loc_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        result_df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])

        for page in range(self.get_page_count(ram_loc_url.format(pg='1'))):
            r = requests.get(ram_loc_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                req_result = r.json().get('results')

                for i in range(len(req_result)):
                    result_df.loc[len(result_df) + i] = [req_result[i]['id'],
                                                         req_result[i]['name'],
                                                         req_result[i]['type'],
                                                         req_result[i]['dimension'],
                                                         len(req_result[i]['residents'])]
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        result_df = result_df.sort_values(by='resident_cnt', ascending=False).head(3)
        print("result_df =", result_df)
        str_res = ""
        for i, row in result_df.iterrows():
            str_res = str_res + f",('{str(row['id'])}','{str(row['name'])}','{str(row['type'])}','{str(row['dimension'])}','{str(row['resident_cnt'])}')"
        str_res = str_res[1:]
        print("str_res =", str_res)

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        sql_statement = f"INSERT INTO d_bashkirtsev_ram_location VALUES " + str_res

        pg_hook.run(sql_statement, False)
