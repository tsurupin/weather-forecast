FROM jupyter/pyspark-notebook
MAINTAINER TOMOAKI TSURUTA (tsurupin@github)

USER root
# Install Tini
RUN wget --quiet https://github.com/krallin/tini/releases/download/v0.10.0/tini && \
    echo "1361527f39190a7338a0b434bd8c88ff7233ce7b9a4876f3315c22fce7eca1b0 *tini" | sha256sum -c - && \
    mv tini /usr/local/bin/tini && \
    chmod +x /usr/local/bin/tini

COPY start-notebook.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/start-notebook.sh
USER $NB_USER
CMD ["start-notebook.sh"]