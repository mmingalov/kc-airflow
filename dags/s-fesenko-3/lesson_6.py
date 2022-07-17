"""
Складываем курс валют в GreenPlum
"""
import airflow.macros
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow import task
import logging
import csv
import xml.etree.ElementTree as ET # Импортировали из библиотеки xml элемент tree и назвали его ET
import os


from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplam
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(10),
    'end_date': days_ago(1),
    'owner': 's-fesenko-3',
    'poke_interval': 600
}

dag = DAG("lesson_6", # Меняем название нашего DAG
          schedule_interval='0 0 * * 1-5',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-fesenko-3']
          )

path = '/tmp'
filepath_xml = '/tmp/s_fesenko_cbr.xml'
filepath_csv = '/tmp/s_fesenko_cbr.csv'

def get_command_func(**context):
    exec_date = context['a-gajdabura']
    if not os.path.isdir(path):
        os.mkdir(path)
    os.chdir(path)
    url = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req='
    forma = airflow.macros.ds_format(exec_date, "%Y-%m-%d", "%d-%m-%Y")
    command = 'curl {url}{date} | iconv -f Windows-1251 -t UTF-8 > {filepath}'.format(
        url=str(url)
        , filepath=str(filepath_xml)
        , date=str(forma)
    )
    logging.info(f'command_get = {command}')
    context['ti'].xcom_push(key = 'get_command', value = command)
    return command

get_command = PythonOperator(
    task_id='get_command',
    python_callable=get_command_func,
    provide_context=True,
    do_xcom_push=True,
    dag=dag
)

def get_request(**context):
    ti = context['ti']
    get_commandd = ti.xcom_pull(task_ids='get_command')
    logging.info(f'get_commandd = {get_commandd}')
    if os.path.exists(filepath_xml):
        os.remove(filepath_xml)
    bash_operator = BashOperator(
        task_id='get_command_for_request',
        bash_command = get_commandd,
        dag=dag
    )
    bash_operator.execute(context=context)
    logging.info(os.path.exists(filepath_xml))


# load_cbr_xml = BashOperator(
#     task_id='load_cbr_xml',
#     bash_command = "echo '%s'" % get_commandd,
#     dag=dag
# )

load_cbr_xml = PythonOperator(
    task_id='load_cbr_xml',
    python_callable=get_request,
    provide_context=True,
    dag=dag
)

def export_xml_to_csv_func():
    if os.path.exists(filepath_csv):
        os.remove(filepath_csv)
    parser = ET.XMLParser(encoding="UTF-8") # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/dina_cbr.xml', parser=parser)
    tree = ET.parse(filepath_xml, parser=parser)
    root = tree.getroot() # Корень нашего файла

    with open(filepath_csv, 'w') as csv_file: # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
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
    logging.info(os.path.exists(filepath_csv))

export_xml_to_csv = PythonOperator( # Xml перекладываем в csv, так как с csv все базы работают гораздо лучше
    task_id='export_xml_to_csv',
    python_callable=export_xml_to_csv_func,
    dag=dag
)

# def delete_data_func():
#     pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
#     sql = f"""delete from s_fesenko_3 where dt::date = '{{ ds_format(a-gajdabura, "%Y-%m-%d", "%d-%m-%y") }}'"""
#     pg_hook.run(sql)
#     # con = pg_hook.get_conn()
#     # cur = con.cursor
#     #
#     # cur.execute(sql)
#     # con.commit()
#     # cur.close()
#     # con.close()
#
# delete_data = PythonOperator(
#     task_id='delete_data',
#     python_callable=delete_data_func,
#     dag=dag
# )

delete_data = PostgresOperator(
    task_id='delete_data',
    postgres_conn_id='conn_greenplum_write',
    database = "karpovcourses",
    sql=[
        "create table if not exists s_fesenko_3 (dt text, id text, num_code text, nominal text, nm text, value text, ts timestamp default now()) distributed by (dt)",
        "truncate table s_fesenko_3"
    ],
    autocommit=True,
    dag=dag
)

def load_csv_to_gp_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write') # Создаем hook, записываем наш гринплан
    pg_hook.copy_expert("COPY s_fesenko_3 FROM STDIN DELIMITER ','", filepath_csv)

load_csv_to_gp = PythonOperator(
   task_id= 'load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

get_command >> load_cbr_xml >> export_xml_to_csv >> delete_data >> load_csv_to_gp