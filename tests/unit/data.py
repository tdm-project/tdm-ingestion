from datetime import datetime, timezone

from tdm_ingestion.tdmq.models import EntityType, Point, Record, Source

now = datetime.now(timezone.utc)

SENSORS_TYPE = [
    EntityType("st1", "cat1"),
    EntityType("st2", "cat2")
]

SENSORS = [
    Source("s1", SENSORS_TYPE[0], Point(0, 1), ["temperature"], "4d9ae10d-df9b-546c-a586-925e1e9ec049"),
    Source("s2", SENSORS_TYPE[1], Point(2, 3), ["humidity"], "6eb57b7e-43a3-5ad7-a4d1-d1ec54bb5520")
]

TIME_SERIES = [
    Record(now, SENSORS[0], Point(0, 1), {"temperature": 14.0}),
    Record(now, SENSORS[1], Point(2, 3), {"humidity": 95.0})
]

# the dictionary returned from the tdmq polystore rest api
REST_SOURCE = {
    "default_footprint": {
        "coordinates": [
            SENSORS[0].geometry.latitude,
            SENSORS[0].geometry.longitude
        ],
        "type": "Point"
    },
    "entity_type": SENSORS_TYPE[0].name,
    "entity_category": SENSORS_TYPE[0].category,
    "external_id": SENSORS[0].id_,
    "stationary": True,
    "tdmq_id": SENSORS[0].tdmq_id
}

REST_TIME_SERIES = {
    "bucket": None,
    "coords": {
        "footprint": [{
            "type": "MultiPoint",
            "coordinates": [[1, 0]]
        }],
        "time": [datetime.timestamp(TIME_SERIES[0].time)]
    },
    "data": {
        "temperature": [14.0]
    },
    "default_footprint": {
        "coordinates": [4.823, 5.166],
        "type": "Point"
    },
    "shape": [],
    "tdmq_id": SENSORS[0].tdmq_id
}
