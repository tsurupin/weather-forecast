FROM python:3.5.0-slim

MAINTAINER TOMOAKI TSURUTA


RUN pip install -U pip

ADD requirements.txt app/requirements.txt
COPY ./app/static /app/static
COPY ./app/app.py /app/app.py

#WORKDIR /app

RUN pip install -r app/requirements.txt
EXPOSE 5000

#FROM tiangolo/uwsgi-nginx-flask:python3.6-index
##RUN yum install -y cronie
#
#ENV MESSAGE "hello from Docker"
#COPY ./app /app
#
#COPY requirements.txt /tmp/
#
## upgrade pip and install required python packages
#RUN pip install -U pip
#RUN pip install -r /tmp/requirements.txt
##
##ADD test-cron /etc/cron.d/test-cron
##RUN chmod 0644 /etc/cron.d/test-cron
##
##ADD test.py /test.py
##RUN chmod +x /test.py
#WORKDIR /app
#ENV MESSAGE "hello from Docker"
##CMD cron && touch /etc/cron.d/test-cron && tail -f /dev/null
##CMD ["python", "app.py"]

#FROM python:3.5.0-slim
#
## Set the file maintainer (your name - the file's author)
##MAINTAINER Chris Page
#COPY ./app /app
#RUN pip install -U pip
## install the python requirements
#ADD requirements.txt requirements.txt
#RUN pip install -r requirements.txt
#
### run the app
#ADD /app/app.py app/app.py
#WORKDIR /app
### expose the port
#EXPOSE 5000
#
### start the command
#CMD ["python", "app.py"]
#
