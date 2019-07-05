SHELL := /bin/bash

ifndef BACKEND
	override BACKEND = confluent
endif

NAME   := tdmproject/tdm-ingestion-${BACKEND}
TAG    := $$(cat VERSION)
BRANCH := $$(git rev-parse --abbrev-ref HEAD | tr -d '\r')
IMG    := $$(if [[ ${BRANCH} == "master" ]]; then  echo ${NAME}:${TAG}; else echo ${NAME}:${TAG}-${BRANCH} ; fi)
LATEST := ${NAME}:latest

images:
	rm docker/tdm_ingestion_dist -rf
	mkdir docker/tdm_ingestion_dist
	cp -a setup.py VERSION tdm_ingestion docker/tdm_ingestion_dist
	docker build -f docker/Dockerfile -t ${IMG} ./docker --build-arg BACKEND=${BACKEND}
	if [[ "${BRANCH}" == "master" ]]; then docker tag ${IMG} ${LATEST}; fi

tests: images
	IMG=${IMG} CONF=${BACKEND} docker-compose -f .travis/docker-compose.yaml up -d


