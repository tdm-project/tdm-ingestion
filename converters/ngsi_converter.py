import datetime
import json
import logging
from typing import List

from ingestion import MessageConverter, Message, TimeSeries, ValueMeasure


class NgsiConverter(MessageConverter):
    def convert(self, messages: List[Message]) -> List[TimeSeries]:
        logging.debug("messages %s", len(messages))
        res = []
        for m in messages:
            try:
                m_dict = json.loads(m.value)
            except json.decoder.JSONDecodeError:
                logging.error('skipping message %s, error while jsondecoding', m.value)
                continue

            _id = m_dict['body']['id']
            time = None
            measures = {}
            for attr in m_dict['body']['attributes']:
                name = attr['name']
                value = attr['value']

                if value is not None and str(value).strip() and \
                        name not in ('dateObserved', 'location', 'latitude', 'longitude'):

                    if name == 'timestamp':
                        time = datetime.datetime.fromtimestamp(float(value), datetime.timezone.utc)
                    else:
                        try:
                            measures[name] = float(value)
                        except Exception as ex:
                            logging.exception(ex)

            if time is None:
                logging.error(f'no valid time/dateObserved in message {m}')
                continue
            for sensor, measure in measures.items():
                res.append(TimeSeries(time, self.get_sensorcode(f'{_id}-{sensor}'), ValueMeasure(measure)))
        return res
