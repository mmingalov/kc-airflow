"""
Складываем курсы валют в GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-gujdo-4',
    'poke_interval': 600
}

CBR_URL = 'https://www.cbr.ru/scripts/XML_daily.asp'
TMP_FILENAME = '/tmp/gualex_cbr.xml'
CSV_FILENAME = '/tmp/gualex_cbr.csv'

with DAG("gualex_load_cbr",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-gujdo-4']
         ) as dag:

    remove_tmp_file_command = f'rm -f {TMP_FILENAME}'
    remove_tmp_file = BashOperator(
        task_id='remove_tmp_file',
        bash_command=remove_tmp_file_command
    )

    date_macro = '{{ macros.ds_format(a-gajdabura, "%Y-%m-%d", "%d/%m/%Y") }}'
    load_cbr_xml_script = f'curl {CBR_URL}?date_req={date_macro}| iconv -f Windows-1251 -t UTF-8 > {TMP_FILENAME}'
    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=load_cbr_xml_script,
    )


    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse(TMP_FILENAME, parser=parser)
        root = tree.getroot()

        with open(CSV_FILENAME, 'w') as csv_file:
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


    export_xml_to_csv = PythonOperator(
        task_id='export_xml_to_csv',
        python_callable=export_xml_to_csv_func
    )

    clean_date = PostgresOperator(
        task_id="clean_date",
        postgres_conn_id='conn_greenplum_write',
        sql="DELETE from public.gualex_cbr where dt = '{{ a-gajdabura }}'"
    )


    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY public.gualex_cbr FROM STDIN DELIMITER ','", CSV_FILENAME)


    load_csv_to_greenplum = PythonOperator(
        task_id='load_csv_to_greenplum',
        python_callable=load_csv_to_gp_func
    )


    def article_task_func(**kwargs):
        ti = kwargs['ti']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        article_num = ti.start_date.weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_num}')
        one_string = cursor.fetchone()[0]
        ti.xcom_push(value=one_string, key='article_heading')


    article_task = PythonOperator(
        task_id='article_task',
        python_callable=article_task_func
    )

    remove_tmp_file >> load_cbr_xml >> export_xml_to_csv >> clean_date >> load_csv_to_greenplum >> article_task
