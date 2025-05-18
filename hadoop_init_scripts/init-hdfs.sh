#!/bin/bash

echo "Attendendo che HDFS esca dalla safe mode..."
until hdfs dfsadmin -safemode get | grep "Safe mode is OFF"; do
  echo "HDFS è ancora in safe mode. Attendo 5 secondi..."
  sleep 5
done
echo "HDFS è uscito dalla safe mode."

echo "Creazione directory e impostazione permessi in HDFS..."

hdfs dfs -mkdir -p /project_data
hdfs dfs -chown nifi:supergroup /project_data
hdfs dfs -chmod 755 /project_data

echo "Inizializzazione HDFS completata."
exit 0