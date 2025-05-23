FROM bitnami/spark:3.5

# Passa all'utente root per installare i pacchetti
USER root

RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir numpy \
    && apt-get purge -y --auto-remove python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Torna all'utente di default di Spark se necessario (l'utente 1001 nelle immagini bitnami)
USER 1001