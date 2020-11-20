FROM python:3.7

ARG BACKEND=confluent
RUN mkdir /tdm

COPY ./setup.py /tdm
COPY ./VERSION /tdm
COPY ./tdm_ingestion /tdm/tdm_ingestion
COPY ./scripts /tdm/scripts

WORKDIR /tdm
RUN pip install --upgrade pip && \
    pip install -e .[$BACKEND]
#ENTRYPOINT ["ingestion.py"]
