import requests
import logging
import pandas as pd

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class PavelEvtuhovichRamLocationOperator(BaseOperator):
    # переопределяем метод __init__ для нашего оператора
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_page_location_count(self, api_url):
        """
        Функция, которая определяет сколько всего страниц в API
        """
        # отправляем запрос по ссылке к API
        res = requests.get(api_url)

        # если статус 200, выводим в лог "Запрос выполнен успешно"
        if res.status_code == 200:
            logging.info(f'HTTP STATUS {res.status_code} - Запрос выполнен успешно')

            # запоминаем значение количества страниц по ключу 'pages' в словаре 'info'
            page_count = res.json().get('info').get('pages')

            # выводи в лог количество страниц
            logging.info(f'page_count = {page_count}')

            # возвращаем количество страниц
            return int(page_count)

        # иначе выводим сообщения об исключении
        else:
            logging.warning(f'HTTP STATUS {res.status_code} - Запрос выполнен с ошибкой')
            raise AirflowException('Error in load page count')

    def execute(self, context):
        """
        Функция, которая собирает данные из API и возвращает список кортежей для записи в БД
        """
        # создаем пустой датафрейм, в который будем записывать значения из словаря 'results'
        df_location_results = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])

        # определяем ссылку на API
        ram_loc_url = 'https://rickandmortyapi.com/api/location?page={pg}'

        # проходим по всем страницам
        for page in range(self.get_page_location_count(ram_loc_url.format(pg='1'))):
            res = requests.get(ram_loc_url.format(pg=str(page + 1)))

            # если запрос выполенен успешно, выводим в лог сообщение с номером страницы
            if res.status_code == 200:
                logging.info(f'PAGE {page + 1}')

                # получаем значения словаря 'results'
                page_results = res.json().get('results')

                # собираем информацию по всем локациям в датафрейм
                for locations in range(len(page_results)):
                    df_location_results.loc[len(df_location_results) + 1] = [page_results[locations]['id'],
                                                                             page_results[locations]['name'],
                                                                             page_results[locations]['type'],
                                                                             page_results[locations]['dimension'],
                                                                             len(page_results[locations]['residents'])]

            # иначе, выводим сообщение об исключении
            else:
                logging.warning(f'HTTP STATUS {res.status_code} - Запрос выполнен с ошибкой')
                raise AirflowException('Error in load from RM API')

        # сортируем датафрейм по убыванию количества резидентов
        df_location_results.sort_values(by='resident_cnt', inplace=True, ascending=False, ignore_index=True)

        # оствляем топ-3
        df_location_results = df_location_results.iloc[:3]

        # переводим в список кортежей для записи в БД
        # по идее, можно было обойтись без датафрейма, а сразу создавать список кортежей и их сортировать в конце
        # но, что сделано, то сделано
        list_to_record = df_location_results.to_records(index=False).tolist()
        logging.info(list_to_record)

        return list_to_record
