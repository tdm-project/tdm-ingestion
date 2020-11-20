SHELL := /bin/bash

IMG ?= tdmproject/tdm-ingestion

images:
	docker build -f Dockerfile -t ${IMG} .

tests: test_kafka test_ckan

test_kafka: images
	cd tests/integration/kafka_consumer; IMG=${IMG} python integration_test.py -d

test_ckan: images
	cd tests/integration/ckan; IMG=${IMG} python integration_test.py -d

tests_no_image: test_kafka_no_image test_ckan_no_image

test_kafka_no_image: images
	cd tests/integration/kafka_consumer; IMG=${IMG} python integration_test.py -d

test_ckan_no_image: images
	cd tests/integration/ckan; IMG=${IMG} python integration_test.py -d