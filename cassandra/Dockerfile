FROM cassandra

MAINTAINER TOMOAKI TSURUTA (tsurupin@github)
USER root

COPY ./migrations /usr/local/
COPY ./seeds /usr/local/

COPY setup.sh /usr/local/bin/setup.sh
RUN chmod +x /usr/local/bin/setup.sh
