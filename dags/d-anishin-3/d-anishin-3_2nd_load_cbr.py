"""
Практика 2: забираем данные из ЦБР и складываем в Гринплам
"""
# Импортируем необходимые библиотеки
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
    'owner': 'd-anishin-3',
    'poke_interval': 600
}

with DAG("d-anishin-3_2nd_load_cbr",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-anishin-3', '2st_practice']
         ) as dag:

    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='curl {url}| iconv -f Windows-1251 -t UTF-8 > /tmp/anisdm_cbr.xml'.format(
            url='https://www.cbr.ru/scripts/XML_daily.asp?date_req=10/12/2021'),
        dag=dag
    )

    def xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/anisdm_cbr.xml', parser=parser)
        root = tree.getroot()  # Корень нашего файла

        with open('/tmp/anisdm_cbr.csv', 'w') as csv_file:
            # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] + [Name] + [Value.replace(',', '.')])
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] + [Name] + [Value.replace(',', '.')])
        # Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку, для того,
        # чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит,
        # что это переход на новое значение
        # Логируем все в log airflow, чтобы посмотреть  все ли хорошо

    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable=xml_to_csv_func
    )

    def load_csv_to_greenplum():
        pg_hook = PostgresHook(postgres_conn_id = 'conn_greenplum_write')
        pg_hook.copy_expert("COPY anisdm_cbr FROM STDIN DELIMITER ','", '/tmp/anisdm_cbr.csv')

    load_csv_to_greenplum = PythonOperator(
        task_id = 'load_csv_to_greenplum',
        python_callable=load_csv_to_greenplum,
    )

    export_cbr_xml >> xml_to_csv >> load_csv_to_greenplum