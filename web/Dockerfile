FROM tiangolo/uwsgi-nginx-flask:python3.6-index
#RUN yum install -y cronie

ENV MESSAGE "hello from Docker"
COPY ./app /app

COPY requirements.txt /tmp/

# upgrade pip and install required python packages
RUN pip install -U pip
RUN pip install -r /tmp/requirements.txt
#
#ADD test-cron /etc/cron.d/test-cron
#RUN chmod 0644 /etc/cron.d/test-cron
#
#ADD test.py /test.py
#RUN chmod +x /test.py
WORKDIR /app

ENV MESSAGE "hello from Docker"
#CMD cron && touch /etc/cron.d/test-cron && tail -f /dev/null
CMD ["python", "cassandra-test.py"]