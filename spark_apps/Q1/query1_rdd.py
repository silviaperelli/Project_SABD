import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    import performance
except ImportError as e:
    print(f"Errore nell'importare 'performance': {e}")
    print(f"sys.path attuale: {sys.path}")


N_RUN = 2

# Query Q1: Aggregare i dati su base annua
# Calcolare media, min, max di "Carbon intensity" e "Carbon-free energy percentage"
# per ciascun anno dal 2021 al 2024.

def run_query1_rdd(spark_session, paths_to_read):
    start_time = time.time()

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {paths_to_read}")
    try:
        # Leggi i Parquet e trasformali in RDD
        df = spark_session.read.parquet(*paths_to_read)
        if df.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {paths_to_read}")
            spark_session.stop()
            exit()

        rdd = df.rdd

        # Mappa i dati per chiave (year, country_code) e raccogli i valori numerici
        mapped_rdd = rdd.map(lambda row: (
            (row['year'], row['country_code']),
            (row['carbon_intensity'], row['carbon_intensity'], row['carbon_intensity'],  # sum, min, max
             row['carbon_free_percentage'], row['carbon_free_percentage'], row['carbon_free_percentage'],  # sum, min, max
             1)  # count
        ))

        # Funzione di riduzione per aggregare i valori
        def reduce_func(a, b):
            return (
                a[0] + b[0], min(a[1], b[1]), max(a[2], b[2]),  # carbon_intensity: sum, min, max
                a[3] + b[3], min(a[4], b[4]), max(a[5], b[5]),  # carbon_free_percentage: sum, min, max
                a[6] + b[6]  # count
            )

        reduced_rdd = mapped_rdd.reduceByKey(reduce_func)

        # Calcola le medie e costruisci una lista di Row
        result_rows = reduced_rdd.map(lambda x: Row(
            date=x[0][0],
            country_code=x[0][1],
            carbon_mean=x[1][0] / x[1][6],
            carbon_min=x[1][1],
            carbon_max=x[1][2],
            cfe_mean=x[1][3] / x[1][6],
            cfe_min=x[1][4],
            cfe_max=x[1][5]
        ))

        # Converte in DataFrame per uniformità e ordinamento
        output_df_q1 = spark_session.createDataFrame(result_rows)
        output_df_q1 = output_df_q1.orderBy("country_code", "date")

        # Scrittura fittizia in formato noop
        output_df_q1.write.format("noop").mode("overwrite").save()

    except Exception as e:
        print(f"Errore durante la lettura o l'elaborazione dei dati: {e}")
        spark_session.stop()
        exit()

    end_time = time.time()

    return output_df_q1, end_time - start_time


if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "2") \
        .getOrCreate()

    sc = spark.sparkContext  # Ottieni SparkContext
    sc.setLogLevel("WARN")

    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read = [
        os.path.join(base_data_path, "country=Italy"),
        os.path.join(base_data_path, "country=Sweden")
    ]

    num_executors_active = spark.conf.get("spark.cores.max")
    print(f"Numero di executors {num_executors_active}")

    execution_times_rdd = []
    output_df_q1_rdd = None

    print(f"\nEsecuzione della Query Q1 con RDD per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q1 RDD - Run {i + 1}/{N_RUN}")
        output_df_q1_rdd, exec_time_rdd = run_query1_rdd(spark, paths_to_read)
        execution_times_rdd.append(exec_time_rdd)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time_rdd:.4f} secondi.")

    avg_time_rdd = performance.print_performance(execution_times_rdd, N_RUN, "Q1 Spark RDD")
    performance.log_performance_to_csv(spark, "Q1", "rdd", avg_time_rdd, 1, N_RUN - 1)

    if output_df_q1_rdd:
        print("\nRisultati finali per Q1 con RDD:")
        output_df_q1_rdd.show(n=output_df_q1_rdd.count(), truncate=False)

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()