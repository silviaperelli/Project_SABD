FROM bitnami/spark:3.5

# Passa all'utente root per installare i pacchetti
USER root

# Aggiorna, installa pip, aggiorna pip, installa redis, poi pulisci la cache di apt
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir redis && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Torna all'utente di default di Spark (1001 nelle immagini bitnami)
USER 1001