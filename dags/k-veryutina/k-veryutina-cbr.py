"""
Складываем курс валют в GreenPlum
Забираем данные поля heading из таблицы articles с id равным дню недели
Работать с понедельника по субботу
"""

from airflow import DAG
from airflow.utils.dates import days_ago,datetime,timedelta
import logging
import csv
import xml.etree.ElementTree as ET

from datetime import datetime as dte

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'krisssver',
    #'retries': 2,
    #'retry_delay': timedelta(seconds=5),
    'sla': timedelta(hours=1),
    'poke_interval': 600
}

xml_file_name ='k-veryutina_cbr.xml'
csv_file_name = 'k-veryutina_cbr.csv'
#dt_api = datetime.today().strftime('%d/%m/%Y')
#dt_db = datetime.today().strftime('%d.%m.%Y')
dt_api = "{{ dag_run.logical_date.strftime('%d/%m/%Y') }}"
dt_db = "{{ dag_run.logical_date.strftime('%d.%m.%Y') }}"
load_cbr_xml_script = f'''
curl https://www.cbr.ru/scripts/XML_daily.asp?date_req={dt_api} | iconv -f Windows-1251 -t UTF-8 > /tmp/{xml_file_name}
'''

dag = DAG("k-veryutina-cbr",
          schedule_interval='0 10 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          catchup=True,
          tags=['krisssver','lesson4']
          )

remove_cbr_xml = BashOperator(
    task_id='remove_cbr_xml',
    bash_command=f'rm -f /tmp/{xml_file_name}',
    dag=dag
)

remove_cbr_csv = BashOperator(
    task_id='remove_cbr_csv',
    bash_command=f'rm -f /tmp/{csv_file_name}',
    dag=dag
)

load_cbr_xml = BashOperator(
    task_id='load_cbr_xml',
    bash_command=load_cbr_xml_script,
    dag=dag
)

def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse(f'/tmp/{xml_file_name}', parser=parser)
    root = tree.getroot()

    with open(f'/tmp/{csv_file_name}', 'w') as csv_file:
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
    python_callable=export_xml_to_csv_func,
    dag=dag
)

def delete_partition(dt_db):
    pg_hook = PostgresHook('conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    logging.info(f"DELETE FROM krisssver_cbr WHERE dt='{str(dt_db)}';")
    cursor.execute(f"DELETE FROM krisssver_cbr WHERE dt='{str(dt_db)}';")

delete_partition = PythonOperator(
    task_id='delete_partition',
    python_callable=delete_partition,
    dag=dag,
    op_args={dt_db}
)

def load_csv_to_gp_func():
    pg_hook = PostgresHook('conn_greenplum')
    pg_hook.copy_expert("COPY krisssver_cbr FROM STDIN DELIMITER ','", f'/tmp/{csv_file_name}')

load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

def get_articles_from_gp(article_id):
    pg_hook = PostgresHook('conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    #article_id = dt.datetime.now().weekday() + 1
    logging.info(str(article_id))
    cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id}')
    query_res = cursor.fetchall()
    logging.info(query_res)

get_articles_from_gp = PythonOperator(
    task_id='get_articles_from_gp',
    python_callable=get_articles_from_gp,
    op_args=['{{ dag_run.logical_date.weekday() + 1 }}'],
    dag=dag)

remove_cbr_xml >> remove_cbr_csv >> load_cbr_xml >> export_xml_to_csv >> delete_partition >> load_csv_to_gp >> get_articles_from_gp