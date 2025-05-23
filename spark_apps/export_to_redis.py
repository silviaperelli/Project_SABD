import os
import json
import redis  # pip install redis
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REDIS_HOST = "redis"  # Nome del servizio nel docker-compose
REDIS_PORT = 6379
HDFS_BASE_PATH = "hdfs://namenode:8020/spark_data/spark"


# crea e restituisce un client redis
def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


def export_q1_to_redis(spark, r):
    print("Esportazione Q1 a Redis...")
    try:
        q1_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q1_results"), header=True, inferSchema=True) # Spark legge i dati dal path specificato
        # Esempio formato output Q1: date, country_code, carbon_mean, carbon_min, carbon_max, cfe_mean, cfe_min, cfe_max
        # 2021, IT, 280.08, 121.24, 439.06, 46.305932, 15.41, 77.02

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
    print("Esportazione Q2 a Redis...")
    # Q2 ha due parti: le classifiche e i dati per i grafici (medie mensili per IT)

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
    print("Esportazione Q3 a Redis...")

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

    # 2. Dati per i grafici di Q3 (medie orarie per IT e SE)
    try:
        hourly_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q3_graphs"), header=True, inferSchema=True)

        collected_q3_hourly = hourly_df.collect()
        pipe = r.pipeline()
        for row in collected_q3_hourly:
            key_ci = f"q3:hourly_avg:{row['country_code']}:carbon_intensity:{row['hour']:02d}"
            pipe.set(key_ci, row['avg_carbon_intensity'])

            key_cfe = f"q3:hourly_avg:{row['country_code']}:cfe:{row['hour']:02d}"
            pipe.set(key_cfe, row['avg_cfe'])
        pipe.execute()
        print(f"Q3 Hourly Averages (IT/SE): {len(collected_q3_hourly) * 2} valori esportati.")  # *2 per CI e CFE
    except Exception as e:
        print(f"Errore durante l'esportazione delle medie orarie Q3: {e}")


def export_q4_to_redis(spark, r):
    print("Esportazione Risultati Clustering a Redis...")
    try:
        clustering_df = spark.read.csv(os.path.join(HDFS_BASE_PATH, "Q4_results"), header=True, inferSchema=True)

        collected_clustering = clustering_df.collect()
        pipe = r.pipeline()

        # Struttura 1: clustering:<country_code> -> cluster_id
        for row in collected_clustering:
            country = row['country_code']
            cluster = row['cluster_prediction']  # Colonna dal clustering script
            key = f"clustering:{country}"
            pipe.set(key, cluster)

        # Struttura 2: clustering:cluster:<cluster_id> -> JSON lista paesi
        clusters_map = {}
        for row in collected_clustering:
            country = row['country_code']
            cluster = row['cluster_prediction']
            if cluster not in clusters_map:
                clusters_map[cluster] = []
            clusters_map[cluster].append(country)

        for cluster_id, countries in clusters_map.items():
            key = f"clustering:cluster:{cluster_id}"
            pipe.set(key, json.dumps(countries))

        pipe.execute()
        print(f"Clustering: {len(collected_clustering)} paesi esportati.")
    except Exception as e:
        print(f"Errore durante l'esportazione dei risultati del Clustering: {e}")

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
    export_q2_to_redis(spark, redis_client)  # Assicurati che Q2_monthly_avg_IT_results sia generato o calcolato
    export_q3_to_redis(spark, redis_client)  # Assicurati che Q3_hourly_avg_results sia generato o calcolato
    export_q4_to_redis(spark, redis_client)  # Assicurati che KMeans_results sia generato

    print("Esportazione a Redis completata.")
    spark.stop()