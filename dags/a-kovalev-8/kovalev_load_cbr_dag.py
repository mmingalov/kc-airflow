"""
забираем данные с CBR.ru, декодируем и сохраняем в GreenPlum fix
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import xml.etree.ElementTree as ET
import csv


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-kovalev-8',
    'poke_interval': 600
}

with DAG("kovalev_load_cbr_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-kovalev-8']
         ) as dag:

    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command=' curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/kovalev_cbr.xml'.format(
            url='https://cbr.ru/scripts/XML_daily.asp?date_req=27/05/2022'
        ),
    )


    def xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/kovalev_cbr.xml', parser=parser)
        root = tree.getroot()

        with open('/tmp/kovalev_cbr.csv', 'w') as csv_file:
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
        python_callable=xml_to_csv_func,
    )


    def load_csv_to_greenplum_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.copy_expert("COPY public.a_kovalev_8_cbr_data FROM STDIN DELIMITER ','", '/tmp/kovalev_cbr.csv')

    load_csv_to_greenplum = PythonOperator(
        task_id='load_csv_to_greenplum',
        python_callable=load_csv_to_greenplum_func
    )



    export_cbr_xml >> xml_to_csv >> load_csv_to_greenplum
