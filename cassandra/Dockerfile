FROM cassandra

MAINTAINER TOMOAKI TSURUTA (tsurupin@github)

USER root
COPY migrations /usr/local/
COPY seeds /user/local/
COPY setup.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/setup.sh

CMD setup.sh
