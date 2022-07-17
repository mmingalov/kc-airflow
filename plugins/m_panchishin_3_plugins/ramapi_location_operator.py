from airflow.models import BaseOperator
from m_panchishin_3_plugins.ramapi_hook import RAMAPILocationHook


class RAMAPILocationOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def execute(self, **kwargs):

        hook = RAMAPILocationHook('dina_ram')
        locations = []
        for location in hook.gen_location_schema():

            row = (
                location['id'],
                location['name'],
                location['type'],
                location['dimension'],
                len(location['residents'])
            )
            self.log.info(tuple(row))
            locations.append(row)

        
        top3_locations = sorted(locations, key=lambda x: x[-1], reverse=True)[:3]
        return_value = ','.join(map(str, top3_locations))
        self.log.info('Found top-3:')
        self.log.info(return_value)

        return return_value
