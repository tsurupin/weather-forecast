FROM python:3.5.0-slim

MAINTAINER TOMOAKI TSURUTA

RUN pip install -U pip

ADD requirements.txt app/requirements.txt

RUN pip install -r app/requirements.txt
EXPOSE 5000
