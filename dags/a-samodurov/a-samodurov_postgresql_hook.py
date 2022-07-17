"""
Складываем курс валют в GreenPlum (Меняем описание нашего дага)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET # Импортировали из библиотеки xml элемент tree и назвали его ET

from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplum
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-samodurov',
    'poke_interval': 600
}

start_date = "{{ execution_date.strftime('%d/%m/%Y') }}"
dt = "{{ execution_date.strftime('%d.%m.%Y') }}"
xml_name = 'a-samodurov_cbr.xml'
csv_name = 'a-samodurov_cbr.csv'
load_cbr_xml_script = f'''
curl https://www.cbr.ru/scripts/XML_daily.asp?date_req={start_date} | iconv -f Windows-1251 -t UTF-8 > /tmp/{xml_name}
'''

dag = DAG("a_samodurov_load_cbr", # Меняем название нашего DAG
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-samodurov']
          )

rm_cbr_xml = BashOperator(
    task_id='rm_cbr_xml',
    bash_command=f'rm -f /tmp/{xml_name}',
    dag=dag
)

load_cbr_xml = BashOperator(
    task_id='load_cbr_xml', # Меняем название в нашем task
    bash_command=load_cbr_xml_script, # Вставляем нашу команду, так как она не помещается в единую строку, то используем форматер url
    dag=dag
)

def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8") # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/dina_cbr.xml', parser=parser)
    tree = ET.parse(f'/tmp/{xml_name}', parser=parser)
    root = tree.getroot() # Корень нашего файла

    with open(f'/tmp/{csv_name}', 'w') as csv_file: # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for Valute in root.findall('Valute'):
            NumCode = Valute.find('NumCode').text
            CharCode = Valute.find('CharCode').text
            Nominal = Valute.find('Nominal').text
            Name = Valute.find('Name').text
            Value = Valute.find('Value').text
            writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                            [Name] + [Value.replace(',', '.')]) # Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку, для того, чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит, что это переход на новое значение
            logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                         [Name] + [Value.replace(',', '.')]) # Логируем все в log airflow, чтобы посмотреть  все ли хорошо

export_xml_to_csv = PythonOperator( # Xml перекладываем в csv, так как с csv все базы работают гораздо лучше
    task_id='export_xml_to_csv',
    python_callable=export_xml_to_csv_func,
    dag=dag
)

def delete_partition_(dt):
    pg_hook = PostgresHook('conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"delete from dina_cbr where dt = '{dt}';")

delete_partition = PythonOperator(
    task_id='delete_partition_',
    python_callable=delete_partition_,
    dag=dag,
    op_args=[dt]
)

def load_csv_to_gp_func():
    pg_hook = PostgresHook('conn_greenplum_write') # Создаем hook, записываем наш гринплан
    pg_hook.copy_expert("COPY dina_cbr FROM STDIN DELIMITER ','", f'/tmp/{csv_name}')

load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

rm_cbr_xml >> load_cbr_xml >> export_xml_to_csv >> delete_partition >> load_csv_to_gp
