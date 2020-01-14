import json
import logging
import unittest

from dateutil.parser import isoparse

from tdm_ingestion.converters.ngsi_converter import (CachedNgsiConverter,
                                                     NgsiConverter)
from tdm_ingestion.tdmq.models import EntityType, Source

logger = logging.getLogger("test_tdm_ingestion")


class TestNgsiConverter(unittest.TestCase):
    def setUp(self):
        self.in_weather_msg = {
            "headers": [{"fiware-service": "tdm"},
                        {"fiware-servicePath": "/cagliari/edge/meteo"},
                        {"timestamp": 1576513894978}],
            "body": {
                "id": "WeatherObserved:Edge-x.esp8266.Davis",
                "type": "WeatherObserved",
                "isPattern": "false",
                "attributes": [
                    {"name": "location", "type": "geo:point", "value": "39.2479168, 9.1329701"},
                    {"name": "dateObserved", "type": "String", "value": "2019-12-16T16:31:34+00:00",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:31:34.943Z"}]
                     },
                    {"name": "windDirection", "type": "Float", "value": "174.545"},
                    {"name": "windSpeed", "type": "Float", "value": "20.0"},
                    {"name": "temperature", "type": "Float", "value": " ",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:31:34.943Z"}]
                     },
                    {"name": "relativeHumidity", "type": "Float", "value": " "},
                    {"name": "precipitation", "type": "Float", "value": " "},
                    {"name": "barometricPressure", "type": "Float", "value": " "},
                    {"name": "illuminance", "type": "Float", "value": " "},
                    {"name": "timestamp", "type": "Integer", "value": "1576513894",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:31:34.943Z"}]
                     },
                    {"name": "infraredLight", "type": "Float", "value": " "},
                    {"name": "CO", "type": "Float", "value": " "},
                    {"name": "NO", "type": "Float", "value": " "},
                    {"name": "NO2", "type": "Float", "value": " "},
                    {"name": "NOx", "type": "Float", "value": " "},
                    {"name": "PM10", "type": "Float", "value": " "},
                    {"name": "PM2.5", "type": "Float", "value": " "},
                    {"name": "SO2", "type": "Float", "value": " "},
                    {"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:31:34.943Z"},
                    {"name": "altitude", "type": "Float", "value": " "},
                    {"name": "rssi", "type": "string", "value": "-36",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:31:34.943Z"}]
                     }
                ]
            }
        }
        self.in_energy_msg = {
            "headers": [
                {"fiware-service": "tdm"},
                {"fiware-servicePath": "/cagliari/edge/energy"},
                {"timestamp": 1576513999532}
            ],
            "body": {
                "type": "EnergyMonitor",
                "isPattern": "false",
                "id": "EnergyMonitor:Edge-y.emontx3.L3",
                "attributes": [
                    {"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:33:19.433Z"},
                    {"name": "apparentPower", "type": "Float", "value": " ",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:33:19.433Z"}]
                     },
                    {"name": "consumedEnergy", "type": "Float", "value": "2.5"},
                    {"name": "current", "type": "Float", "value": " "},
                    {"name": "dateObserved", "type": "String", "value": "2019-12-16T16:33:19+00:00",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:33:19.433Z"}]
                     },
                    {"name": "frequency", "type": "Float", "value": " "},
                    {"name": "location", "type": "geo:point", "value": "0, 0"},
                    {"name": "powerFactor", "type": "Float", "value": "0.4"},
                    {"name": "realPower", "type": "Float", "value": "100"},
                    {"name": "timestamp", "type": "Integer", "value": "1576513999",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:33:19.433Z"}]
                     },
                    {"name": "voltage", "type": "Float", "value": " "},
                    {"name": "rssi", "type": "string", "value": "-36",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-16T16:33:19.433Z"}]
                     }
                ]
            }
        }
        self.in_device_msg = {
            "headers": [
                {"fiware-service": "tdm"},
                {"fiware-servicePath": "/cagliari/edge/device"},
                {"timestamp": 1576677906834}
            ],
            "body": {
                "type": "DeviceStatus",
                "isPattern": "false",
                "id": "DeviceStatus:Edge-z.EDGE.HTU21D",
                "attributes": [
                    {"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-18T14:05:06.823Z"},
                    {"name": "atmosphericPressure", "type": "Float", "value": " "},
                    {"name": "cpuCount", "type": "Integer", "value": " "},
                    {"name": "dateObserved", "type": "String", "value": "2019-12-18T14:05:05+00:00",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-18T14:05:06.823Z"}]},
                    {"name": "diskFree", "type": "Float", "value": " "},
                    {"name": "diskTotal", "type": "Float", "value": " "},
                    {"name": "humidity", "type": "Float", "value": "47.48968505859375",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-18T14:05:06.823Z"}]},
                    {"name": "illuminance", "type": "Float", "value": " "},
                    {"name": "kernelRelease", "type": "String", "value": " "},
                    {"name": "kernelVersion", "type": "String", "value": " "},
                    {"name": "lastBoot", "type": "String", "value": " "},
                    {"name": "location", "type": "geo:point", "value": "0, 0",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-18T14:05:06.823Z"}]},
                    {"name": "memoryFree", "type": "Float", "value": " "},
                    {"name": "memoryTotal", "type": "Float", "value": " "},
                    {"name": "operatingSystem", "type": "String", "value": " "},
                    {"name": "swapFree", "type": "Float", "value": " "},
                    {"name": "swapTotal", "type": "Float", "value": " "},
                    {"name": "systemArchitecture", "type": "String", "value": " "},
                    {"name": "temperature", "type": "Float", "value": "20.578688964843742",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-18T14:05:06.823Z"}]},
                    {"name": "timestamp", "type": "Integer", "value": "1576677905",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-18T14:05:06.823Z"}]},
                    {"name": "latitude", "type": "string", "value": "0",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-18T14:05:06.823Z"}]},
                    {"name": "longitude", "type": "string", "value": "0",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-18T14:05:06.823Z"}]},
                    {"name": "dewpoint", "type": "string", "value": "9.088586800582902",
                     "metadatas": [{"name": "TimeInstant", "type": "ISO8601", "value": "2019-12-18T14:05:06.823Z"}]}
                ]
            }
        }

    def _test_convert_weather(self, converter):
        timeseries_list = converter.convert([json.dumps(self.in_weather_msg)])
        self.assertEqual(len(timeseries_list), 1)
        self.assertEqual(timeseries_list[0].source.type, EntityType("WeatherObserver", "Station"))
        self.assertEqual(timeseries_list[0].data, {
            "windDirection": 174.545,
            "windSpeed": 20.0,
            "rssi": -36
        })
        self.assertEqual(timeseries_list[0].time.strftime("%Y-%m-%dT%H:%M:%SZ"), "2019-12-16T16:31:34Z")
        self.assertEqual(str(timeseries_list[0].source.id_), "Edge-x.esp8266.Davis")

    def _test_convert_energy(self, converter):
        timeseries_list = converter.convert([json.dumps(self.in_energy_msg)])
        self.assertEqual(len(timeseries_list), 1)
        self.assertEqual(timeseries_list[0].source.type, EntityType("EnergyConsumptionMonitor", "Station"))
        self.assertEqual(timeseries_list[0].data, {
            "consumedEnergy": 2.5,
            "powerFactor": 0.4,
            "realPower": 100.0,
            "rssi": -36
        })
        self.assertEqual(timeseries_list[0].time.strftime("%Y-%m-%dT%H:%M:%SZ"), "2019-12-16T16:33:19Z")
        self.assertEqual(str(timeseries_list[0].source.id_), "Edge-y.emontx3.L3")

    def _test_convert_device(self, converter):
        timeseries_list = converter.convert([json.dumps(self.in_device_msg)])
        self.assertEqual(len(timeseries_list), 1)
        self.assertEqual(timeseries_list[0].source.type, EntityType("DeviceStatusMonitor", "Station"))
        self.assertEqual(timeseries_list[0].data, {
            "humidity": 47.48968505859375,
            "temperature": 20.578688964843742,
            "dewpoint": 9.088586800582902,
        })
        self.assertEqual(timeseries_list[0].time.strftime("%Y-%m-%dT%H:%M:%SZ"), "2019-12-18T14:05:05Z")
        self.assertEqual(str(timeseries_list[0].source.id_), "Edge-z.EDGE.HTU21D")

    def _test_convert_error(self, message):
        timeseries_list = NgsiConverter().convert(message)
        self.assertEqual(len(timeseries_list), 0)

    def _test_convert_runtime_error(self, message):
        self._test_convert_error([json.dumps(message)])

    def test_convert_weather(self):
        self._test_convert_weather(NgsiConverter())

    def test_convert_energy(self):
        self._test_convert_energy(NgsiConverter())

    def test_convert_device(self):
        self._test_convert_device(NgsiConverter())

    def test_json_decode_error(self):
        self._test_convert_error(["(a)"])

    def test_runtime_error_wrong_id(self):
        self.in_weather_msg["body"]["id"] = "WrongId"
        self._test_convert_runtime_error(self.in_weather_msg)

        self.in_energy_msg["body"]["id"] = "WrongId"
        self._test_convert_runtime_error(self.in_weather_msg)

    def test_runtime_error_missing_geometry_attributes(self):
        for msg in (self.in_energy_msg, self.in_weather_msg):
            for attr in msg["body"]["attributes"]:
                if attr["name"] == "location":
                    msg["body"]["attributes"].remove(attr)
            self._test_convert_runtime_error(msg)

    def test_runtime_error_missing_header(self):
        for msg in (self.in_energy_msg, self.in_weather_msg):
            for header in msg["headers"]:
                if "fiware-servicePath" in header:
                    msg["headers"].remove(header)
            self._test_convert_runtime_error(msg)

    def test_cached_convert(self):
        converter = CachedNgsiConverter()
        self.assertEqual(len(converter.sensors), 0)

        self._test_convert_weather(converter)
        self._test_convert_energy(converter)
        self.assertEqual(len(converter.sensors), 2)
        self.assertTrue(isinstance(list(converter.sensors.values())[0], Source))


if __name__ == "__main__":
    unittest.main()
