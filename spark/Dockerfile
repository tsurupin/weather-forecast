FROM jupyter/pyspark-notebook
MAINTAINER TOMOAKI TSURUTA (tsurupin@github)

USER root

COPY start-notebook.sh /usr/local/bin/

RUN chmod a+x /usr/local/bin/start-notebook.sh
USER $NB_USER
CMD ["start-notebook.sh"]