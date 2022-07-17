"""Забираем данные из CBR и складываем в Greenplum"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import xml.etree.ElementTree as ET
import csv
import os
import pandas as pd
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'mahw',
    'poke_interval': 600
}

with DAG("ma_load_CBR",
    start_date=days_ago(2),
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['mahw']
) as dag:

    def today_date_func(execution_date):
        date_format=datetime.strptime(execution_date,'%Y-%m-%d')
        return datetime.strftime(date_format, '%d/%m/%Y')

    today_date=PythonOperator(
        task_id='today-date',
        python_callable=today_date_func,
        op_kwargs= {'execution_date': '{{a-gajdabura}}'}
    )

    export_cbr_XML=BashOperator(
        task_id='export_cbr_XML',
        bash_command='curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/cbr.xml'.format(
        url='https://www.cbr.ru/scripts/XML_daily.asp?date_req='+'{{ ti.xcom_pull(task_ids="today-date") }}'
    ))

    def xml_to_csv_func(**kwargs):
        parser=ET.XMLParser(encoding='UTF-8')
        tree=ET.parse('/tmp/cbr.xml', parser=parser)
        root=tree.getroot()
        today_date=kwargs['templates_dict']['implicit']
        today_date = today_date.replace('/', '-')
        with open('/tmp/cbr'+today_date+'.csv','w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',','.')])
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',', '.')])


    xml_to_csv=PythonOperator(
        task_id='cbr_to_csv',
        python_callable=xml_to_csv_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="today-date") }}'}
    )

    def merge_two_csv_func(**kwargs):
        today_date = kwargs['templates_dict']['implicit']
        today_date = today_date.replace('/', '-')
        a = pd.DataFrame(columns=["dt", "id", "num_code", "char_code", "nominal", "nm", "value"])
        file = True
        try:
            with open("/tmp/cbr.csv", 'r') as output_file:
                pass
        except FileNotFoundError:
            file=False
        if file:
            a = pd.read_csv('/tmp/cbr.csv')
            a.columns=["dt", "id", "num_code", "char_code","nominal","nm","value"]
        b = pd.read_csv('/tmp/cbr'+today_date+'.csv')
        b.columns = ["dt", "id", "num_code", "char_code", "nominal", "nm", "value"]
        merged = a.merge(b, how='outer')
        merged.to_csv("/tmp/cbr.csv", index=False)
        os.remove('/tmp/cbr'+today_date+'.csv')

    merge_two_csv=PythonOperator(
        task_id='merge_two_csv',
        python_callable=merge_two_csv_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="today-date") }}'}
    )

    def load_csv_to_greenplum_func():
        pg_hook=PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('TRUNCATE TABLE averbakh_cbr')
        pg_hook.copy_expert("COPY averbakh_cbr from STDIN DELIMITER ','",'/tmp/cbr.csv')


    load_csv_to_greenplum=PythonOperator(
        task_id='load_csv_to_greenplum',
        python_callable=load_csv_to_greenplum_func,
    )

    today_date >> export_cbr_XML >> xml_to_csv >> merge_two_csv >> load_csv_to_greenplum