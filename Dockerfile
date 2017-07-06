# Telepat.io Worker image
#
# VERSION 0.1.2

FROM node:4.8.3
MAINTAINER Andrei Marinescu <andrei@telepat.io>

RUN mkdir /app

COPY . /app
WORKDIR /app
RUN npm install

WORKDIR /app

EXPOSE 3000
