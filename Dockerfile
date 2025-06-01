FROM bitnami/spark:3.5

# Passa all'utente root per installare i pacchetti
USER root

RUN /opt/bitnami/python/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/bitnami/python/bin/pip install --no-cache-dir redis numpy

# Torna all'utente di default di Spark (1001 nelle immagini bitnami)
USER 1001
