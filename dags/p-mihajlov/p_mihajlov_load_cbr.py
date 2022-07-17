"""
Забираем данные из ЦБР и складываем в GP
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import xml.etree.ElementTree as ET
import csv
from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 4, 10),
    'owner': 'p-mihajlov',
    'poke_interval': 600
}

with DAG("p_mihajlov_load_cbr",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p-mihajlov']
         ) as dag:

    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/dina_cbr.xml'.format(
            url='https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/11/2021'
        ),
        dag=dag
    )

    def xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/dina_cbr.xml', parser=parser)
        root = tree.getroot()

        with open('/tmp/dina_cbr.csv', 'w') as csv_file:
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
        dag=dag
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook(postgres_conn_id = 'conn_greenplum_write')
        pg_hook.copy_expert("COPY p_mihajlov_cbr FROM STDIN DELIMITER ','", '/tmp/dina_cbr.csv')


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        dag=dag
    )

    export_cbr_xml >> xml_to_csv >> load_csv_to_gp

    # dag.doc_md = __doc__
    # wait_until_6am.doc_md = """Сенсор. Ждёт наступления 6am по Гринвичу"""
    # echo_ds.doc_md = """Пишет в лог execution_date"""
    # first_task.doc_md = """Пишет в лог 'First  log'"""
