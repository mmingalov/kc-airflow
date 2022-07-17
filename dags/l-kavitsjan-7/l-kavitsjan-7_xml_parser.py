# coding=utf-8
"""
Даг для скачивания данных о котировках валют с сайта ЦБ РФ

- задача на скачивание файла XML

- задача на парсинг XML

- задача на запись результатов в файл csv

- задача на загрузку файла в БД Greenplum
"""

import os
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from contextlib import closing

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'l-kavitsjan-7',
    'poke_interval': 600,
    'email': ['myworkpython@mail.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
}

# TODO: сделать дату выгрузки не константой а заменить на шаблон с текущей датой ({{ format_ds ... }})
start_date = "{{ execution_date.strftime('%d/%m/%Y') }}"
dt = "{{ execution_date.strftime('%d.%m.%Y') }}"


# TODO: сделать проверку наличия скачанного файла CSV
# TODO: сделать проверку наличия скачанного файла XML
def check_file_exist(file_name):
    # Проверка наличия файла
    if os.path.exists(file_name):
        os.remove(file_name)
        logging.info('''
            --------------------------------------------------------------------------------------------
            Файл {file_name} уже существует! Произведено его удаление перед скачиванием нового
            --------------------------------------------------------------------------------------------
            '''.format(file_name=file_name))
    else:
        logging.info('''
            --------------------------------------------------------------------------------------------
            Загружен файл с котировками {file_name}.
            --------------------------------------------------------------------------------------------
            '''.format(file_name=file_name))


url = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req={start_date}'.format(
    start_date=start_date,
)

# TODO: внести url файлы с xml и csv в константу
XML_FILE_NAME = '/tmp/kavitsjan_cbr_values.xml'
CSV_FILE_NAME = '/tmp/kavitsjan_cbr_values.csv'
TABLE_NAME = 'klm_cbr_valutes'

load_cbr_xml_script = 'curl {url} | iconv -f Windows-1251 -t UTF-8 > {file_name}'.format(
    url=url,
    file_name=XML_FILE_NAME,
)


def export_xml_to_csv_func():
    # Создаем Parser формата UTF-8, натравливаем его на нужный файл
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse(XML_FILE_NAME, parser=parser)
    # Корень нашего файла
    root = tree.getroot()

    # Открываем csv, в которую будем писать построчно каждый элемент: Valute, NumCode и т.д.
    with open(CSV_FILE_NAME, 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for Valute in root.findall('Valute'):
            # Из атрибута root берем дату, из атрибута valute берем id,
            # в конце заменяем запятую на точку, для того, чтобы при сохранении в формате csv,
            # если оставить запятую в нашем поле, формат решит, что это переход на новое значение
            NumCode = Valute.find('NumCode').text
            CharCode = Valute.find('CharCode').text
            Nominal = Valute.find('Nominal').text
            Name = Valute.find('Name').text
            Value = Valute.find('Value').text
            writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                            [Name] + [Value.replace(',', '.')])
            # Записываем все в log airflow, чтобы посмотреть все ли хорошо
            logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                         [Name] + [Value.replace(',', '.')])


def load_csv_to_gp_func(file_name, table_name):
    # Создаем хук для проверки наличия записей в таблице с валютами
    pg_hook_read = PostgresHook('conn_greenplum')
    conn = pg_hook_read.get_conn()
    # TODO: Мёрдж или очищение предыдущего батча
    with open(file_name, 'r+') as f:
        with closing(conn) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute('select count(*) from {table_name};'.format(table_name=table_name))
                result = cur.fetchall()
                logging.info('''
                            --------------------------------------------------------------------------------------------
                            Результат выгрузки из БД: {fetch}.
                            --------------------------------------------------------------------------------------------
                            '''.format(fetch=result))
                if result[0][0] == 0:
                    pass
                else:
                    cur.execute('delete from {table_name};'.format(table_name=TABLE_NAME))
                conn.commit()

    # Создаем hook, записываем валюты в Greenplum
    pg_hook_write = PostgresHook('conn_greenplum_write')
    pg_hook_write.copy_expert("COPY {table_name} FROM STDIN DELIMITER ','".format(
        table_name=TABLE_NAME,
    ), CSV_FILE_NAME)


with DAG(
    'l-kavitsjan-7_load_cbr_values',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['l-kavitsjan-7'],
) as dag:

    with TaskGroup(group_id='check_group') as check_group:
        # Проверка наличия файла, если он уже есть, удаляем его
        PythonOperator(
            task_id='check_xml_file_exist_op',
            op_args=[XML_FILE_NAME],
            python_callable=check_file_exist,
            dag=dag
        )
        # Проверяем наличие файла CSV, если он уже есть, удаляем его
        PythonOperator(
            task_id='check_csv_file_exist_op',
            op_args=[CSV_FILE_NAME],
            python_callable=check_file_exist,
            dag=dag
        )

    load_cbr_xml = BashOperator(
        # Меняем название в нашем task
        task_id='load_cbr_xml',
        # Вставляем нашу команду, так как она не помещается в единую строку, то используем форматер url
        bash_command=load_cbr_xml_script,
        dag=dag,
    )

    # Xml перекладываем в csv, так как с csv все базы работают гораздо лучше
    export_xml_to_csv = PythonOperator(
        task_id='export_xml_to_csv',
        python_callable=export_xml_to_csv_func,
        dag=dag
    )

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        op_args=[CSV_FILE_NAME, TABLE_NAME],
        python_callable=load_csv_to_gp_func,
        dag=dag
    )

    check_group >> load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp

