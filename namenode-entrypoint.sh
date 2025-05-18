#!/bin/bash

/entrypoint.sh /run.sh &

echo "Attendendo che il NameNode RPC (8020) sia disponibile..."
while ! nc -z localhost 8020; do
  sleep 1
done
echo "NameNode RPC è disponibile."

/opt/hadoop_init_scripts/init-hdfs.sh

wait $!