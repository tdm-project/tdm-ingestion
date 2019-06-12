FROM python:3.7
COPY ./tdm_ingestion_dist /tdm
WORKDIR /tdm
RUN pip install --upgrade pip && \
    pip install -e .[confluent-kafka]
ENTRYPOINT ["ingestion.py"]
