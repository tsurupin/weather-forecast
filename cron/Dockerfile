FROM python:3.5.0-slim

MAINTAINER TOMOAKI TSURUTA

COPY ./app /app
ADD requirements.txt /app/

WORKDIR /app
RUN pip install -r requirements.txt
