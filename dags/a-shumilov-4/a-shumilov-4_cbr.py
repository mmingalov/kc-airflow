"""cbr даг"""

from airflow import DAG, macros
from airflow.utils.dates import days_ago
from datetime import datetime
import logging
import csv
import os
import os.path
import xml.etree.ElementTree as ET

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

URL = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req={{ macros.ds_format(a-gajdabura, "%Y-%m-%d", "%d/%m/%Y") }}'
XML_FILE = '/tmp/shumilov_cbr.xml'
CSV_FILE = '/tmp/shumilov_cbr.csv'
load_cbr_xml_script = 'curl {} | iconv -f Windows-1251 -t UTF-8 > {}'.format(URL, XML_FILE)

DEFAULT_ARGS = {
    'start_date': days_ago(8),
    'owner': 'a-shumilov-4',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': 10,
    'priority_weight': 2,
    'end_date': datetime(2022, 6, 7)

}

dag = DAG("a-shumilov-cbr",
          schedule_interval='0 1 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-shumilov']
          )



load_cbr_xml = BashOperator(
    task_id='load_cbr_xml',
    bash_command=load_cbr_xml_script,
    dag=dag
)


def delete_file(file):
    if os.path.isfile(file):
        os.remove(file)


def delete_files():
    delete_file(XML_FILE)
    delete_file(CSV_FILE)

delete_temp_files = PythonOperator(
    task_id='delete_temp_files',
    python_callable=delete_files,
    dag=dag
)

def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse(XML_FILE, parser=parser)
    root = tree.getroot()


    with open(CSV_FILE, 'w') as csv_file:
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
    return root.attrib['Date']

export_xml_to_csv = PythonOperator(
    task_id='export_xml_to_csv',
    python_callable=export_xml_to_csv_func,
    dag=dag
)


def load_csv_to_gp_func(**kwargs):
    pg_hook = PostgresHook('conn_greenplum_write')
    conn = pg_hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor() # ("named_cursor_name")
    logging.info("DELETE FROM public.shumilov_cbr WHERE dt = '{}'".format(kwargs['templates_dict']['implicit']))
    cursor.execute("DELETE FROM public.shumilov_cbr WHERE dt = '{}'".format(kwargs['templates_dict']['implicit']))
    conn.close()
    pg_hook.copy_expert("COPY public.shumilov_cbr FROM STDIN DELIMITER ','", CSV_FILE)

load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="export_xml_to_csv") }}'},
    provide_context=True,
    dag=dag
)

delete_temp_files >> load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp
