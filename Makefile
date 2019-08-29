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

test_kafka_consumer:
	cd tests/integration_tests/kafka_consumer; IMG=${IMG} CONF=${BACKEND} python integration_test.py


