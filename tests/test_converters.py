import json
import unittest

from tdm_ingestion.converters.ngsi_converter import NgsiConverter
from tdm_ingestion.ingestion import Message


class TestNgsiConverter(unittest.TestCase):
    message = {
        "headers": [{"fiware-service": "tdm"}, {"fiware-servicePath": "/cagliari/edge/meteo"},
                    {"timestamp": 1531774294021}],
        "body": {
            "attributes": [
                {"name": "barometricPressure", "type": "float", "value": " "},
                {"name": "dateObserved", "type": "String", "value": "2018-07-16T20:51:33+00:00"},
                {"name": "location", "type": "geo:point", "value": "39.2479168, 9.1329701"},
                {"name": "timestamp", "type": "Integer", "value": "1531774293"},
                {"name": "windDirection", "type": "Float", "value": "174.545"},
                {"name": "windSpeed", "type": "Float", "value": "0.000"},
                {"name": "latitude", "type": "string", "value": "39.2479168"},
                {"name": "longitude", "type": "string", "value": "9.1329701"}
            ],
            "type": "WeatherObserved",
            "isPattern": "false", "id": "WeatherObserved:Edge-CFA703F4.esp8266-7806085.Davis"
        }
    }

    def test_convert(self):
        timeseries_list = NgsiConverter().convert([Message('', json.dumps(TestNgsiConverter.message))])
        self.assertEqual(len(timeseries_list), 2)
        self.assertEqual(timeseries_list[0].measure.value, 174.545)
        self.assertEqual(timeseries_list[0].time.strftime('%Y-%m-%dT%H:%M:%S'), '2018-07-16T20:51:33')
        self.assertEqual(str(timeseries_list[0].sensor.name), 'esp8266-7806085.Davis.windDirection')

        self.assertEqual(timeseries_list[1].measure.value, 0.0)
        self.assertEqual(timeseries_list[1].time.strftime('%Y-%m-%dT%H:%M:%S'), '2018-07-16T20:51:33')
        self.assertEqual(str(timeseries_list[1].sensor.name), 'esp8266-7806085.Davis.windSpeed')


if __name__ == '__main__':
    unittest.main()
