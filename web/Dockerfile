FROM tiangolo/uwsgi-nginx-flask:python3.6-index
ENV MESSAGE "hello from Docker"
COPY ./app /app

COPY requirements.txt /tmp/

# upgrade pip and install required python packages
RUN pip install -U pip
RUN pip install -r /tmp/requirements.txt

WORKDIR /app

ENV MESSAGE "hello from Docker"