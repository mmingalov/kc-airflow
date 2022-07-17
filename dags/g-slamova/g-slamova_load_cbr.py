"""
Складываем курс валют в GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'g-slamova',
    'poke_interval': 600
}

url = 'https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021'
xml_file = '/tmp/dina_cbr.xml'
csv_file = '/tmp/dina_cbr.csv'

with DAG("g-slamova_load_cbr",
          schedule_interval='0 0 * * mon-sat',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['g-slamova']
          ) as dag:

    delete_xml_file_script = f'rm {xml_file}'

    delete_xml_file = BashOperator(
        task_id='delete_xml_file',
        bash_command=delete_xml_file_script,
        trigger_rule='dummy'
        )

    delete_csv_file_script = f'rm {csv_file}'

    delete_csv_file = BashOperator(
        task_id='delete_csv_file',
        bash_command=delete_csv_file_script,
        trigger_rule='dummy'
    )

    load_cbr_xml_script = f'curl {url} | iconv -f Windows-1251 -t UTF-8 > {xml_file}'

    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=load_cbr_xml_script
    )

    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse(xml_file, parser=parser)
        root = tree.getroot()

        with open(csv_file, 'w') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
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
        python_callable=export_xml_to_csv_func
    )

    def load_csv_to_gp_func(**kwargs):
        pg_hook = PostgresHook('conn_greenplum_write')
        conn = pg_hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()
        logging.info("DELETE FROM public.dina_cbr WHERE dt = '{}'".format(kwargs['templates_dict']['implicit']))
        cursor.execute("DELETE FROM public.dina_cbr WHERE dt = '{}'".format(kwargs['templates_dict']['implicit']))
        conn.close()
        pg_hook.copy_expert("COPY dina_cbr FROM STDIN DELIMITER ','", csv_file)


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="export_xml_to_csv") }}'},
        provide_context=True
    )

load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp