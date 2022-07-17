"""
Cbr practice
https://lab.karpov.courses/learning/83/module/1006/lesson/8571/25054/108899/
"""
import random

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

import csv
import xml.etree.ElementTree as ET  # Импортировали из библиотеки xml элемент tree и назвали его ET

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'd-mihajlov-8',
    'depends_on_past': True
}

URL = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req={{ macros.ds_format(ds, "%Y-%m-%d", "%d/%m/%Y") }}'
FILE_NAME = 'dmmikhaylov_cbr'
XML_PATH = f'/tmp/{FILE_NAME}.xml'
CSV_PATH = f'/tmp/{FILE_NAME}.csv'


def export_xml_to_csv_func():
    # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/dina_cbr.xml', parser=parser)
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse(XML_PATH, parser=parser)
    root = tree.getroot()  # Корень нашего файла

    # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
    with open(CSV_PATH, 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for Valute in root.findall('Valute'):
            NumCode = Valute.find('NumCode').text
            CharCode = Valute.find('CharCode').text
            Nominal = Valute.find('Nominal').text
            Name = Valute.find('Name').text
            Value = Valute.find('Value').text
            # Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку, для того,
            # чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит, что это переход
            # на новое значение
            row = [root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + \
                  [CharCode] + [Nominal] + [Name] + [Value.replace(',', '.')]
            writer.writerow(row)
            # Логируем все в log airflow, чтобы посмотреть  все ли хорошо
            logging.info(row)


def branch(**kwargs):
    tasks = []
    day = kwargs['execution_date'].weekday()
    if day != 6:
        tasks = ['export_xml']
    logging.info(f'Day number is {day}')
    return tasks


def load_csv_to_gp_func():
    pg = PostgresHook('conn_greenplum')

    sql_delete = f"""
        DELETE
        FROM karpovcourses.public.dmmikhaylov_cbr 
        WHERE dt = '{{ ds }}'
        """

    logging.info(f'SQL_delete: {sql_delete}')
    # del old data
    with pg.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_delete)

    # insert new data
    pg.copy_expert(f"COPY {FILE_NAME} FROM STDIN DELIMITER ','", CSV_PATH)


with DAG(
        dag_id='d-mihajlov-8_load_cbr',
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['shek']
) as dag:
    start = DummyOperator(
        task_id='start'
    )

    branch_op = BranchPythonOperator(
        task_id='choose_day',
        python_callable=branch,
        provide_context=True
    )

    curl_command = 'curl  {URL} | iconv -f Windows-1251 -t UTF-8 > {DL_PATH}'.format(
        URL=URL,
        DL_PATH=XML_PATH
    )
    logging.info(f'Curl command:{curl_command}')

    export_xml = BashOperator(
        task_id='export_xml',
        bash_command=curl_command
    )

    export_xml_to_csv = PythonOperator(
        task_id='export_xml_to_csv',
        python_callable=export_xml_to_csv_func
    )

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    del_raw_files = BashOperator(
        task_id='del_raw_files',
        bash_command=f'rm {XML_PATH} && rm {CSV_PATH}'
    )

    test = BashOperator(
        task_id='test',
        bash_command=f'echo {URL}'
    )

    start >> test >> branch_op >> export_xml >> export_xml_to_csv >> load_csv_to_gp >> del_raw_files
