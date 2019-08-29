SHELL := /bin/bash

ifndef BACKEND
	override BACKEND = sync
endif
IMG ?= tdmproject/tdm-ingestion

images:
	rm docker/tdm_ingestion_dist -rf
	mkdir docker/tdm_ingestion_dist
	cp -a setup.py VERSION tdm_ingestion docker/tdm_ingestion_dist
	docker build -f docker/Dockerfile -t ${IMG} ./docker --build-arg BACKEND=${BACKEND}

tests: images test_kafka_consumer
	#for i in $(seq 1 20); do docker exec travis_web_1 curl http://localhost:8000/api/v0.0/sensors;if [[ "$?" -ne 0  ]]; then sleep 5; else break; fi; done

	#for test in `ls .`; do cd ${test}; IMG=${IMG} CONF=${BACKEND} docker-compose up -d; python integration_test.py; docker-compose down; done
	cd tests/integration_tests; for test in `ls .`; python integration_test.py; done


test_kafka_consumer:
	cd tests/integration_tests/kafka_consumer; IMG=${IMG} CONF=${BACKEND} python integration_test.py


