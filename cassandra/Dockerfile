FROM cassandra

MAINTAINER TOMOAKI TSURUTA (tsurupin@github)
USER root

RUN mkdir /usr/local/migrations \
    && mkdir /usr/local/seeds \
    && mkdir /usr/local/raw_data
COPY ./migrations /usr/local/migrations
COPY ./seeds /usr/local/seeds
COPY ./raw_data /usr/local/raw_data

COPY setup.sh /usr/local/bin/setup.sh
RUN chmod +x /usr/local/bin/setup.sh
