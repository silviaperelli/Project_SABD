#!/bin/bash

echo "Attendendo che HDFS esca dalla safe mode..."
until hdfs dfsadmin -safemode get | grep "Safe mode is OFF"; do
  echo "HDFS è ancora in safe mode. Attendo 5 secondi..."
  sleep 5
done
echo "HDFS è uscito dalla safe mode."

echo "Creazione directory e impostazione permessi in HDFS..."
echo "Creazione directory per l'output di NiFi: /nifi_data"

hdfs dfs -mkdir -p /nifi_data
hdfs dfs -chown nifi:supergroup /nifi_data

echo "Creazione directory per Spark: /spark_data"

hdfs dfs -mkdir -p /spark_data
hdfs dfs -chown spark:supergroup /spark_data

echo "Inizializzazione HDFS completata."
exit 0




