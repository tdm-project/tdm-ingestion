SHELL := /bin/bash

IMG ?= tdmproject/tdm-ingestion

images:
	rm docker/tdm_ingestion_dist -rf
	mkdir docker/tdm_ingestion_dist
	cp -a setup.py VERSION tdm_ingestion scripts docker/tdm_ingestion_dist
	docker build -f docker/Dockerfile -t ${IMG} ./docker

tests: test_kafka test_ckan

test_kafka: images
	cd tests/integration/kafka_consumer; IMG=${IMG} python integration_test.py

test_ckan: images
	cd tests/integration/ckan; IMG=${IMG} python integration_test.py

test_no_image: test_kafka_no_image test_ckan_no_image

test_kafka_no_image: images
	cd tests/integration/kafka_consumer; IMG=${IMG} python integration_test.py

test_ckan_no_image: images
	cd tests/integration/ckan; IMG=${IMG} python integration_test.py