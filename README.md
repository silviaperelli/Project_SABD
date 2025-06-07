# Progetto SABD - Analisi di Dati Energetici con Apache Spark

Lo scopo del progetto è usare il framework di data processing Apache Spark per rispondere ad alcune query su dati storici forniti da Electricity Maps sull’elettricità e sulle emissioni di CO2 per produrla.

Per eseguire il progetto è necessario aver installato sul proprio dispositivo locale Docker Compose e Python.
Passi per l'esecuzione:

## Creazione dell'ambiente containerizzato
Dalla cartella del progetto eseguire i seguenti comandi: 

1. Avvio dei container
   
        docker-compose up -d
   
2. Verifica dello stato dei container
   
        docker-compose ps

## Apache Nifi
1. All'indirizzo "https://localhost:8443/nifi" accedere all'UI di Nifi
2. Importare il template "Data_Ingestion.xml"
3. Avviare il flusso Nifi per il caricamento dei dati su HDFS

## Apache Spark
Dalla cartella del progetto eseguire i seguenti comandi: 

1. Preprocessamento dei dati
   
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --driver-java-options "-Divy.home=/tmp/.ivy_home_spark_driver" \
        --conf spark.ivy.home=/tmp/.ivy_home_spark_conf \
        /opt/spark_apps/preprocess_data.py 
   
2. Esecuzione query i-esima
   
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --driver-java-options "-Divy.home=/tmp/.ivy_home_spark_driver" \
        --conf spark.ivy.home=/tmp/.ivy_home_spark_conf \
        /opt/spark_apps/main.py <i>

2. Esportazione risultati e perfomance su Redis 
   
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --driver-java-options "-Divy.home=/tmp/.ivy_home_spark_driver" \
        --conf spark.ivy.home=/tmp/.ivy_home_spark_conf \
        /opt/spark_apps/export_to_redis.py

## Grafana
1. All'indirizzo "http://localhost:3000/" accedere all'UI di Grafana
2. Creare un Data Source Redis con indirizzo "redis:6379"
3. Importare la dashboard "Dashboard_SABD.json" presente nella cartella "grafana" del progetto e selezionare il Data Source Redis creato al punto 2
4. Visualizzare i grafici nel pannello della dashboard
