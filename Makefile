NAME   := tdmproject/tdm-ingestion
TAG    := $$(cat VERSION)
IMG    := ${NAME}:${TAG}
LATEST := ${NAME}:latest


images:
	rm docker/tdm_ingestion_dist -rf
	mkdir docker/tdm_ingestion_dist
	cp -a setup.py VERSION tdm_ingestion docker/tdm_ingestion_dist
	docker build -f docker/Dockerfile -t ${IMG} ./docker
	docker tag ${IMG} ${LATEST}


