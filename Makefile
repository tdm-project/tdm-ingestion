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

tests: images
	IMG=${IMG} CONF=${BACKEND} docker-compose -f .travis/docker-compose.yaml up -d
	for i in $(seq 1 20); do docker exec travis_web_1 curl http://localhost:8000/api/v0.0/sensors;if [[ "$?" -ne 0  ]]; then sleep 5; else break; fi; done
	cd tests; python integration_tests.py
