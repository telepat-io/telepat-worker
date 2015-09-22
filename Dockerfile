# Telepat.io Worker image
#
# VERSION 0.1.2

FROM node:0.12
MAINTAINER Andrei Marinescu <andrei@telepat.io>

RUN mkdir /app

COPY . /app
WORKDIR /app
RUN npm install
# Apache Kafka, Elasticsearch and Couchbase default settings
ENV TP_ES_HOST 127.0.0.1
ENV TP_ES_PORT 9200
ENV TP_ES_INDEX default

ENV TP_KFK_HOST 127.0.0.1
ENV TP_KFK_PORT 2181
ENV TP_KFK_CLIENT "telepat-worker"

ENV TP_REDIS_HOST 127.0.0.1
ENV TP_REDIS_PORT 6379

ENV TP_MAIN_DB "ElasticSearch"
ENV TP_PW_SALT \$2a\$10\$N9qo8uLOickgx2ZMRZoMye

WORKDIR /app

EXPOSE 3000
