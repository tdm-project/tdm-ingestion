images:
	rm docker/tdm_ingestion_dist -rf
	mkdir docker/tdm_ingestion_dist
	cp setup.py docker/tdm_ingestion_dist
	cp -a tdm_ingestion docker/tdm_ingestion_dist
	docker build -f docker/Dockerfile -t tdm/ingestion ./docker


