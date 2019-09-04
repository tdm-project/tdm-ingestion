SHELL := /bin/bash

ifndef BACKEND
	override BACKEND = sync
endif
IMG ?= tdmproject/tdm-ingestion

images:
	rm docker/tdm_ingestion_dist -rf
	mkdir docker/tdm_ingestion_dist
	cp -a setup.py VERSION tdm_ingestion scripts docker/tdm_ingestion_dist
	docker build -f docker/Dockerfile -t ${IMG} ./docker --build-arg BACKEND=${BACKEND}

tests: test_kafka_consumer test_ckan

test_kafka_consumer: images
	cd tests/integration_tests/kafka_consumer; IMG=${IMG} CONF=${BACKEND} python integration_test.py

test_ckan: images
	if [[ ${BACKEND} == 'sync' ]]; then cd tests/integration_tests/ckan; IMG=${IMG} CONF=${BACKEND} python integration_test.py; fi

