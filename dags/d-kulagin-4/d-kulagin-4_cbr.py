"""
Load CRB currencies on daily basis schedule and put data into GreenPlum database
"""

import csv
import logging
import xml.etree.ElementTree as ET
from datetime import date, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # noqa
from airflow.utils.dates import days_ago

_TMP_FILENAME_PREFIX = '/tmp/d_kulagin_4_cbr'

_DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-kulagin-4',
    'poke_interval': 600
}

def get_yesterday_date():
    yesterday = date.today() - timedelta(days=2)
    return yesterday.strftime('%d/%m/%Y')


def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse(f'{_TMP_FILENAME_PREFIX}.xml', parser=parser)
    root = tree.getroot()

    with open(f'{_TMP_FILENAME_PREFIX}.csv', 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for Valute in root.findall('Valute'):
            NumCode = Valute.find('NumCode').text
            CharCode = Valute.find('CharCode').text
            Nominal = Valute.find('Nominal').text
            Name = Valute.find('Name').text
            Value = Valute.find('Value').text
            writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                            [Name] + [Value.replace(',', '.')])
            logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                         [Name] + [Value.replace(',', '.')])


def load_csv_to_greenplum():
    pg_hook = PostgresHook(postgres_conn_id = 'conn_greenplum_write')
    pg_hook.copy_expert("COPY public.d_kulagin_4_cbr FROM STDIN DELIMITER ','", f'{_TMP_FILENAME_PREFIX}.csv')


with DAG("d-kulagin-4_load_cbr",
         schedule_interval='@daily',
         default_args=_DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-kulagin-4', 'load_cbr']) as dag:

    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='curl {url} | iconv -f Windows-1251 -t UTF-8 > {filename}'.format(
            url=f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={get_yesterday_date()}',
            filename=f'{_TMP_FILENAME_PREFIX}.xml',
        ),
        dag=dag,
    )

    xml2csv = PythonOperator(
        task_id='xml2csv',
        python_callable=export_xml_to_csv_func,
        dag=dag,
    )

    csv2GP = PythonOperator(
        task_id='csv2GP',
        python_callable=load_csv_to_greenplum,
        dag=dag,
    )

    export_cbr_xml >> xml2csv >> csv2GP
