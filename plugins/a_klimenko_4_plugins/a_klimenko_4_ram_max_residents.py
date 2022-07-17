import requests
import json

from airflow.models import BaseOperator



class AllaRamMaxResidentsOperator(BaseOperator):
    """
    Сохранение в XCom топ 3 локации с наибольшим количеством резидентов
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        # Локации забираются единственным запросом, без деления на страницы
        r = requests.get('https://rickandmortyapi.com/api/location')

        # Конвертируем в json, запоминаем только значимую часть ответа
        locations = json.loads(r.text)['results']

        # Соберём локации в массив словарей
        location_list = []
        for location in locations:
            location_dict = {
                'id': location['id'],
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            location_list.append(location_dict)

        # Отсортируем этот массив и выберем три первых элемента
        sorted_locations = sorted(location_list,
                                  key=lambda cnt: cnt['resident_cnt'],
                                  reverse=True)
        top3_location = sorted_locations[:3]
        return top3_location
