import requests
import logging
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


def get_page_count(api_url):
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
        return page_count
    else:
        logging.warning("HTTP STATUS {}".format(r.status_code))
        raise AirflowException('Error in load page count')


def get_top_three_rows(pd_df):
    output = pd_df.sort_values('residents_cnt', ascending=False).head(3)
    return output


def get_location_data_on_page(result_json):
    col_names = ['id', 'name', 'type', 'dimension', 'residents_cnt']
    df_page = pd.DataFrame(columns=col_names)
    for location in result_json:
        df = pd.json_normalize(location)
        df['residents_cnt'] = df['residents'].str.len()
        df = df.drop(columns=['residents', 'url', 'created'])
        df_page = pd.concat([df_page, df], ignore_index=True)
    df_page = get_top_three_rows(df_page)
    return df_page


def push_data_into_gp_func(locations):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    values = []
    for i in range(0, locations['id'].count()):
        row = locations.iloc[i].values.tolist()
        string = "(" + str(i+1) + ", '" + "','".join(str(cell) for cell in row) + "')"
        values.append(string)

    values_insert = ', '.join(str(line) for line in values)
    sql_insert = f'''insert into "e-pavlov-8_ram_location"
        (system_key, id, name, type, dimension, residents_cnt) values ''' + values_insert

    logging.info('SQL INSERT QUERY: ' + sql_insert)
    cursor.execute(sql_insert)
    conn.commit()


class PavlovTop3Locations(BaseOperator):
    """
    Count top 3 location by residents.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        """
        Import top 3 location by residents into GreenPlum
        """
        columns_lst_all = ['id', 'name', 'type', 'dimension', 'residents_cnt']
        df_all = pd.DataFrame(columns=columns_lst_all)
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                df_all = pd.concat([df_all, get_location_data_on_page(r.json().get('results'))], ignore_index=True)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        df_top = get_top_three_rows(df_all)
        push_data_into_gp_func(df_top)
