import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

N_RUN = 10

# Query Q1: Aggregare i dati su base annua
# Calcolare media, min, max di "Carbon intensity" e "Carbon-free energy percentage"
# per ciascun anno dal 2021 al 2024.

def run_query1(spark_session, paths_to_read):
    start_time = time.time()

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {paths_to_read}")
    try:
        # Lettura dei dati Parquet specificando una lista di path
        df_processed = spark_session.read.parquet(*paths_to_read)

        if df_processed.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {paths_to_read}")
            spark_session.stop()
            exit()

    except Exception as e:
        print(f"Errore durante la lettura dei dati dalle partizioni: {e}")
        spark_session.stop()
        exit()

    # Aggregazione per anno
    annual_aggregated_df = df_processed.groupBy("year", "country_code") \
        .agg(
        F.avg("carbon_intensity").alias("carbon_mean"),
        F.min("carbon_intensity").alias("carbon_min"),
        F.max("carbon_intensity").alias("carbon_max"),
        F.avg("carbon_free_percentage").alias("cfe_mean"),
        F.min("carbon_free_percentage").alias("cfe_min"),
        F.max("carbon_free_percentage").alias("cfe_max")
    ) \
        .orderBy("country_code", "year")

    annual_aggregated_df_renamed = annual_aggregated_df.withColumnRenamed("year", "date")

    output_df_q1 = annual_aggregated_df_renamed.select(
        "date", "country_code",
        "carbon_mean", "carbon_min", "carbon_max",
        "cfe_mean", "cfe_min", "cfe_max"
    )

    # Azione per forzare l'esecuzione e misurare il tempo
    output_df_q1.write.format("noop").mode("overwrite").save()

    end_time = time.time()

    return output_df_q1, end_time - start_time


def query1_df(num_executor):
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", num_executor) \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read = [
        os.path.join(base_data_path, "country=Italy"),
        os.path.join(base_data_path, "country=Sweden")
    ]

    execution_times = []  # Lista per memorizzare i tempi di ogni esecuzione della query
    final_output_df_q1 = None  # Per salvare il risultato dell'ultima esecuzione

    print(f"\nEsecuzione della Query Q1 con DataFrame per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q1 DataFrame - Run {i + 1}/{N_RUN}")

        result_df, exec_time = run_query1(spark, paths_to_read)
        execution_times.append(exec_time)
        print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
        if i == N_RUN - 1:
            final_output_df_q1 = result_df


    avg_time = performance.print_performance(execution_times, N_RUN, "Q1")
    performance.log_performance_to_csv(spark, "Q1", "dataframe", avg_time, num_executor)

    if final_output_df_q1:
        print("\nRisultati aggregati finali per Q1 con DataFrame:")

        final_output_df_q1.show(n=final_output_df_q1.count(), truncate=False)

        csv_output_path = os.path.join(base_data_path, "Q1_results")  # Path per il CSV
        # .coalesce(1) riduce il numero di partizioni a 1 per scrivere un singolo file CSV
        final_output_df_q1.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")
        print(f"Risultati Q1 salvati in CSV: {csv_output_path}")


    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()