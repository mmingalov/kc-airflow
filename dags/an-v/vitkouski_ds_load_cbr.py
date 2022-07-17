"""
Load currency rates and save to GP
"""

import csv
import logging
import xml.etree.ElementTree as ET

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'an-v',
    'poke_interval': 600
}

with DAG("vitkouski_ds_load_cbr",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['an-v']
         ) as dag:
    url = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req=04/02/2022'
    extract_cbr = BashOperator(
        task_id='extract_cbr',
        bash_command=f'curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/an-v-cbr.xml'
    )


    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/an-v-cbr.xml', parser=parser)
        root = tree.getroot()

        with open('/tmp/an-v-cbr.csv', 'w') as csv_file:
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


    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable=export_xml_to_csv_func
    )

    def load_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY dina_cbr FROM STDIN DELIMITER','", '/tmp/an-v-cbr.csv')

    load_to_gp = PythonOperator(
        task_id='load_to_gp',
        python_callable=load_to_gp_func
    )

    extract_cbr >> xml_to_csv >> load_to_gp
