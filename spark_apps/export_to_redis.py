import os
import json
import redis
from pyspark.sql import SparkSession

REDIS_HOST = "redis"  # Nome del servizio nel docker-compose
REDIS_PORT = 6379
HDFS_BASE_PATH = "hdfs://namenode:8020/spark_data/spark"

# crea e restituisce un client redis
def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


def export_q1_to_redis(spark, r):
    print("\nEsportazione risultati Q1 su Redis...")
    try:
        q1_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q1_results"), header=True, inferSchema=True)

        collected_q1 = q1_df.collect() # raccolta dei dati sul driver Spark
        pipe = r.pipeline() # pipeline Redis
        for row in collected_q1: # itero sulle righe e accedo ai valori delle colonne 'date' e 'country_code'
            key = f"q1:{row['date']}:{row['country_code']}" # costruisce la chiave Redis
            value = json.dumps(row.asDict()) # serializza il dizionario Python in una stringa JSON
            pipe.set(key, value) # accoda il comando SET <key> <value> alla pipeline ma non lo invia ancora a Redis
        pipe.execute() # esecuzione della pipeline
        print(f"Q1: {len(collected_q1)} righe esportate.")
    except Exception as e:
        print(f"Errore durante l'esportazione di Q1: {e}")


def export_q2_to_redis(spark, r):
    print("\nEsportazione risultati Q2 su Redis...")

    # 1. Classifiche (prime 5)
    try:
        q2_ranks_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q2_results"), header=True, inferSchema=True)

        collected_q2_ranks = q2_ranks_df.collect()
        rank_types = ["ci_desc", "ci_asc", "cfe_desc", "cfe_asc"]
        pipe = r.pipeline()
        for i, row in enumerate(collected_q2_ranks):
            rank_type_idx = i // 5
            rank_num = (i % 5) + 1
            if rank_type_idx < len(rank_types):
                key = f"q2:italy:rank:{rank_types[rank_type_idx]}:{rank_num}"
                value = json.dumps(row.asDict())  # Contiene 'date', 'carbon_intensity', 'cfe'
                pipe.set(key, value)
        pipe.execute()
        print(f"Q2 Ranks: {len(collected_q2_ranks)} righe esportate.")

    except Exception as e:
        print(f"Errore durante l'esportazione delle classifiche Q2: {e}")

    # 2. Dati per i grafici di Q2 (medie mensili per l'Italia)
    try:
        monthly_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q2_graphs"), header=True, inferSchema=True)

        collected_q2_monthly = monthly_df.collect()
        pipe = r.pipeline()
        for row in collected_q2_monthly:
            key = f"q2:italy:monthly_avg:{row['date']}"  # es. q2:italy:monthly_avg:2022_12
            value = json.dumps(row.asDict())
            pipe.set(key, value)
        pipe.execute()
        print(f"Q2 Monthly Averages (IT): {len(collected_q2_monthly)} righe esportate.")

    except Exception as e:
        print(f"Errore durante l'esportazione delle medie mensili Q2: {e}")


def export_q3_to_redis(spark, r):
    print("\nEsportazione risultati Q3 su Redis...")

    # 1. Statistiche
    try:
        q3_stats_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q3_results"), header=True, inferSchema=True)

        collected_q3_stats = q3_stats_df.collect()
        pipe = r.pipeline()
        for row in collected_q3_stats:
            key = f"q3:stats:{row['country_code']}:{row['data']}"
            value_dict = row.asDict()
            del value_dict['country_code']
            del value_dict['data']
            value = json.dumps(value_dict)
            pipe.set(key, value)
        pipe.execute()
        print(f"Q3 Stats: {len(collected_q3_stats)} righe esportate.")
    except Exception as e:
        print(f"Errore durante l'esportazione delle statistiche Q3: {e}")

    # 2. Dati per i grafici di Q3 (medie orarie per IT e SE) - MODIFICATO
    try:
        hourly_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q3_graphs"), header=True, inferSchema=True)
        # Assicurati che hourly_df abbia le colonne: country_code, hour, avg_carbon_intensity, avg_cfe

        collected_q3_hourly = hourly_df.collect()
        pipe = r.pipeline()
        for row in collected_q3_hourly:
            country = row['country_code']
            hour_val = int(row['hour'])  # Assicurati che hour sia un intero per il formato :02d
            hour_str_padded = f"{hour_val:02d}"  # Es. "00", "01", ..., "23"
            artificial_timestamp = f"2021-01-01T{hour_str_padded}:00:00Z"

            data_to_store = row.asDict()
            data_to_store['time'] = artificial_timestamp

            key_ci = f"q3:hourly_avg:{country}:{hour_str_padded}"
            value = json.dumps(data_to_store)
            pipe.set(key_ci, value)

        pipe.execute()
        print(f"Q3 Hourly Averages (IT/SE): {len(collected_q3_hourly)} JSON objects esportati.")
    except Exception as e:
        print(f"Errore durante l'esportazione delle medie orarie Q3: {e}")


def export_q4_silhouette_to_redis(spark, r):
    print("\nEsportazione Risultati Clustering con Silhouette Score su Redis...")
    try:
        clustering_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q4_silhouette_results"), header=True, inferSchema=True)
        silhouette_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "silhouette_values"), header=True, inferSchema=True)

        collected_clustering = clustering_df.collect()
        collected_silhouette = silhouette_df.collect()
        pipe_cluster = r.pipeline()
        pipe_tuning = r.pipeline()

        # Struttura: clustering:silhouette:<country_code> -> cluster_id
        for row in collected_clustering:
            country = row['country_code']
            key = f"clustering:silhouette:{country}"
            data_to_store = row.asDict()
            value = json.dumps(data_to_store)
            pipe_cluster.set(key, value)

        pipe_cluster.execute()

        # Struttura: silhouette:<k> -> score
        for row in collected_silhouette:
            k = row['k']
            key = f"silhouette:{k}"
            data_to_store = row.asDict()
            value = json.dumps(data_to_store)
            pipe_tuning.set(key, value)

        pipe_tuning.execute()

        print(f"Clustering: {len(collected_clustering)} paesi esportati.")
    except Exception as e:
        print(f"Errore durante l'esportazione dei risultati del Clustering: {e}")


def export_q4_elbow_to_redis(spark, r):
    print("\nEsportazione Risultati Clustering con Elbow Method su Redis...")
    try:
        clustering_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q4_elbow_results"), header=True, inferSchema=True)
        elbow_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "elbow_values"), header=True, inferSchema=True)

        collected_clustering = clustering_df.collect()
        collected_elbow = elbow_df.collect()
        pipe_cluster = r.pipeline()
        pipe_tuning = r.pipeline()

        # Struttura: clustering:elbow:<country_code> -> cluster_id
        for row in collected_clustering:
            country = row['country_code']
            key = f"clustering:elbow:{country}"
            data_to_store = row.asDict()
            value = json.dumps(data_to_store)
            pipe_cluster.set(key, value)

        pipe_cluster.execute()

        # Struttura: elbow:<k> -> score
        for row in collected_elbow:
            k = row['k']
            key = f"elbow:{k}"
            data_to_store = row.asDict()
            value = json.dumps(data_to_store)
            pipe_tuning.set(key, value)

        pipe_tuning.execute()

        print(f"Clustering: {len(collected_clustering)} paesi esportati.")
    except Exception as e:
        print(f"Errore durante l'esportazione dei risultati del Clustering: {e}")

def export_performance_to_redis(spark, r):
    print("\nEsportazione dati di performance su Redis...")
    total_rows_exported = 0

    query_data_path = os.path.join(HDFS_BASE_PATH, "performance/*/*/*/*.csv")

    try:
        performance_df = spark.read.csv(query_data_path, header=True, inferSchema=True)
        collected_data = performance_df.collect()

        if not collected_data:
            print(f"Nessun dato trovato nella directory")
            return 0

        pipe = r.pipeline()
        query_rows = 0
        for row in collected_data:
            key = f"performance:{row['query_name']}:{row['query_type']}:{row['num_executors']}"
            data_to_store = row.asDict()
            value = json.dumps(data_to_store)
            pipe.set(key, value)
            query_rows += 1

        pipe.execute()
        total_rows_exported += query_rows

    except Exception as e:
        print(f"Errore durante l'esportazione dei dati delle performance: {e}")

    print(f"Esportazione completata. Totale righe esportate su Redis: {total_rows_exported}")

def hdfs_path_exists(spark_session, path):
    # path should be "hdfs://namenode:8020/..."
    sc = spark_session.sparkContext
    uri = sc._jvm.java.net.URI(path)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, sc._jsc.hadoopConfiguration())
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ExportToRedis") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ottiene il client Redis e testa la connessione
    redis_client = None
    try:
        redis_client = get_redis_client()
        redis_client.ping()  # verifica connessione inviando un ping
        print("Connesso a Redis!")
    except redis.exceptions.ConnectionError as e:
        print(f"Impossibile connettersi a Redis: {e}")
        spark.stop()
        exit(1)

    export_q1_to_redis(spark, redis_client)
    export_q2_to_redis(spark, redis_client)
    export_q3_to_redis(spark, redis_client)
    export_q4_silhouette_to_redis(spark, redis_client)
    export_q4_elbow_to_redis(spark, redis_client)
    export_performance_to_redis(spark, redis_client)

    print("Esportazione a Redis completata.")
    spark.stop()