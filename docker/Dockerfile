FROM python:3.7

ARG BACKEND=confluent
COPY ./tdm_ingestion_dist /tdm
WORKDIR /tdm
RUN pip install --upgrade pip && \
    pip install -e .[$BACKEND]
ENTRYPOINT ["ingestion.py"]
