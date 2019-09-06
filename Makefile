SHELL := /bin/bash

IMG ?= tdmproject/tdm-ingestion

images:
	rm docker/tdm_ingestion_dist -rf
	mkdir docker/tdm_ingestion_dist
	cp -a setup.py VERSION tdm_ingestion scripts docker/tdm_ingestion_dist
	docker build -f docker/Dockerfile -t ${IMG} ./docker

tests: test_kafka test_ckan

test_kafka: images
	cd tests/integration_tests/kafka_consumer; IMG=${IMG} python integration_test.py

test_ckan: images
	cd tests/integration_tests/ckan; IMG=${IMG} python integration_test.py

