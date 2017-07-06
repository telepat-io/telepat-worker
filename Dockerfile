# Telepat.io Worker image
#
# VERSION 0.1.2

FROM node:0.12
MAINTAINER Andrei Marinescu <andrei@telepat.io>

RUN mkdir /app

COPY . /app
WORKDIR /app
RUN npm install

WORKDIR /app

EXPOSE 3000
