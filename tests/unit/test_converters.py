import json
import logging
import unittest

from tdm_ingestion.converters.ngsi_converter import (CachedNgsiConverter,
                                                     NgsiConverter)
from tdm_ingestion.tdmq.models import Source

logger = logging.getLogger('test_tdm_ingestion')

class TestNgsiConverter(unittest.TestCase):
    message = {
        "headers": [{"fiware-service": "tdm"},
                    {"fiware-servicePath": "/cagliari/edge/meteo"},
                    {"timestamp": 1531774294021}],
        "body": {
            "attributes": [
                {"name": "barometricPressure", "type": "float", "value": " "},
                {"name": "dateObserved", "type": "String",
                 "value": "2018-07-16T20:51:33+00:00"},
                {"name": "location", "type": "geo:point",
                 "value": "39.2479168, 9.1329701"},
                {"name": "timestamp", "type": "Integer",
                 "value": "1531774293"},
                {"name": "windDirection", "type": "Float", "value": "174.545"},
                {"name": "windSpeed", "type": "Float", "value": "0.000"},
                {"name": "latitude", "type": "string", "value": "39.2479168"},
                {"name": "longitude", "type": "string", "value": "9.1329701"}
            ],
            "type": "WeatherObserved",
            "isPattern": "false",
            "id": "WeatherObserved:Edge-CFA703F4.esp8266-7806085.Davis"
        }
    }

    def _test_convert(self, converter):
        timeseries_list = converter.convert([json.dumps(self.message)])
        self.assertEqual(len(timeseries_list), 1)
        logger.debug(timeseries_list[0].to_json())
        self.assertEqual(timeseries_list[0].data,
                         {'windDirection': 174.545, 'windSpeed': 0.0})
        self.assertEqual(timeseries_list[0].time.strftime('%Y-%m-%dT%H:%M:%S'),
                         '2018-07-16T20:51:33')
        self.assertEqual(str(timeseries_list[0].source.id_),
                         'esp8266-7806085.Davis')

    def _test_convert_error(self, message):
        timeseries_list = NgsiConverter().convert(message)
        self.assertEqual(len(timeseries_list), 0)

    def _test_convert_runtime_error(self, message):
        self._test_convert_error([json.dumps(message)])

    def test_ngsi_convert(self):
        self._test_convert(NgsiConverter())

    def test_json_decode_error(self):
        self._test_convert_error(['(a)'])

    def test_runtime_error_wrong_id(self):
        message = {
            "headers": [{"fiware-service": "tdm"},
                        {"fiware-servicePath": "/cagliari/edge/meteo"},
                        {"timestamp": 1531774294021}],
            "body": {
                "id": "WrongId"
            }
        }
        self._test_convert_runtime_error(message)

    def test_runtime_error_missing_geometry_attributes(self):
        message = {
            # missing latitute and longitude
            "headers": [{"fiware-service": "tdm"},
                        {"fiware-servicePath": "/cagliari/edge/meteo"},
                        {"timestamp": 1531774294021}],
            "body": {
                "attributes": [],
                "type": "WeatherObserved",
                "isPattern": "false",
                "id": "WeatherObserved:Edge-CFA703F4.esp8266-7806085.Davis"
            }
        }
        self._test_convert_runtime_error(message)

    def test_runtime_error_missing_header(self):
        message = {
            # missing fiware-servicePath
            "headers": [{"fiware-service": "tdm"},
                        {"timestamp": 1531774294021}],
            "body": {
                "attributes": [
                    {"name": "latitude", "type": "string", "value": "39.2479168"},
                    {"name": "longitude", "type": "string", "value": "9.1329701"}
                ],
                "type": "WeatherObserved",
                "isPattern": "false",
                "id": "WeatherObserved:Edge-CFA703F4.esp8266-7806085.Davis"
            }
        }
        self._test_convert_runtime_error(message)

    def test_cached_convert(self):
        converter = CachedNgsiConverter()
        self.assertEqual(len(converter.sensors), 0)

        self._test_convert(converter)

        self.assertEqual(len(converter.sensors), 1)

        self.assertTrue(
            isinstance(list(converter.sensors.values())[0], Source))


if __name__ == '__main__':
    unittest.main()
