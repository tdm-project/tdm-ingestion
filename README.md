###### Description
This repo contains code fro ingesting data from topic kafka containing ngsi json message to tdmq.

###### Installing
pip install --user -e .[confluent-kafka]

```
ingestion.py conf.yaml
```

where an example of conf.yaml is right below:

```
consumer:
  class: tdm_ingestion.consumers.confluent_kafka_consumer.KafkaConsumer
  args:
    bootstrap_servers:
      - kafka:9092
    topics:
      - test
storage:
  class: tdm_ingestion.storage.tdmq_storage.TDMQStorage
  args:
    tdmq_url: http://web:8000

ingester:
  process:
    timeout_s: 20
    max_records: 10
```

###### Docker
A docker image can be build running

```
make images
```

Then it can be run:

```
docker run -v /path/to/conf.yaml:/opt/tdm_ingestion/conf.yaml tdm/ingestion /opt/tdm_ingestion/conf.yaml

```

