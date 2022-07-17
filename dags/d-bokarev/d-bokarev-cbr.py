"""
DAG для загрузки данных с сайта cbr.ru в GreenPlum
"""
import logging

from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.decorators import task

import requests
import xml.etree.ElementTree as ET
import pandas as pd

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'poke_interval': 600,
    'owner': 'd-bokarev'
}
with DAG("d-bokarev-cbr",
         default_args=DEFAULT_ARGS,
         schedule_interval='@daily',
         tags=['d-bokarev']) as dag:
    @task
    def get_data_from_cbr(**kwargs):
        request_date = kwargs['execution_date'].strftime('%d/%m/%Y')
        url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req= {request_date}'
        response = requests.get(url)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            rows = []
            cols = ["dt", "id", "num_code", "char_code", "nominal", "nm", "value"]
            for child in root.findall('Valute'):
                NumCode = child.find('NumCode').text
                CharCode = child.find('CharCode').text
                Nominal = child.find('Nominal').text
                Name = child.find('Name').text
                Value = child.find('Value').text
                rows.append({'dt': root.attrib['Date'],
                             'id': child.attrib['ID'],
                             'num_code': NumCode,
                             'char_code': CharCode,
                             'nominal': Nominal,
                             'nm': Name,
                             'value': Value.replace(',', '.')})
            df = pd.DataFrame(rows, columns=cols)
            df.to_csv('/tmp/d_bokarev_cbr.csv', header=False, index=False)
            kwargs['ti'].xcom_push(value=root.attrib['Date'], key='last_response_date')
            return True
        return False

    @task
    def push_data_to_greenplum(res,**kwargs):
        if res:
            dt = kwargs['ti'].xcom_pull(task_ids='get_data_from_cbr', key='last_response_date')
            gp_hook = PostgresHook('conn_greenplum_write')
            try:
                with gp_hook.get_conn() as conn:
                    with conn.cursor() as curs:
                        curs.execute(f"delete from d_bokarev_cbr where dt = '{dt}';")
            finally:
                conn.close()

            logging.info('Start copy data from csv...')
            gp_hook.copy_expert("COPY d_bokarev_cbr FROM STDIN DELIMITER ','", '/tmp/d_bokarev_cbr.csv')
            logging.info('Success!')


    push_data_to_greenplum(get_data_from_cbr())