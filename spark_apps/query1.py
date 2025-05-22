import statistics
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

N_RUN = 10

# Query Q1: Aggregare i dati su base annua
# Calcolare media, min, max di "Carbon intensity" e "Carbon-free energy percentage"
# per ciascun anno dal 2021 al 2024.

def run_query1(spark_session, paths_to_read):
    start_time = time.time()

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {paths_to_read}")
    try:
        # Leggi i dati Parquet specificando una lista di path
        df_processed = spark_session.read.parquet(*paths_to_read)

        if df_processed.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {paths_to_read}")
            spark_session.stop()
            exit()

    except Exception as e:
        print(f"Errore durante la lettura dei dati dalle partizioni: {e}")
        spark_session.stop()
        exit()

    print("\nEsecuzione della Query Q1...")

    # Esegui l'aggregazione
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

    output_df_q1.write.format("noop").mode("overwrite").save()

    end_time = time.time()

    return output_df_q1, end_time - start_time


if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read = [
        os.path.join(base_data_path, "country=Italy"),
        os.path.join(base_data_path, "country=Sweden")
    ]

    execution_times = []  # Lista per memorizzare i tempi di ogni esecuzione della query
    final_output_df_q1 = None  # Per salvare il risultato dell'ultima esecuzione

    print(f"\nEsecuzione della Query Q1 per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"Esecuzione Q1 - Run {i + 1}/{N_RUN}")

        result_df, exec_time = run_query1(spark, paths_to_read)
        execution_times.append(exec_time)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
        if i == N_RUN - 1:  # Se è l'ultima esecuzione, salva il DataFrame risultato
            final_output_df_q1 = result_df

    # Calcola e stampa le statistiche dei tempi di esecuzione
    if execution_times:
        avg_time = statistics.mean(execution_times)
        print(f"\n--- Statistiche Tempi Esecuzione Query Q1 ({N_RUN} runs) ---")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times]}")
        print(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")
        if len(execution_times) > 1:  # La deviazione standard richiede almeno 2 campioni
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard dei tempi: {std_dev_time:.4f} secondi")
        print("----------------------------------------------------")

    if final_output_df_q1:
        print("\nRisultati aggregati finali per Q1:")

        final_output_df_q1.show(n=final_output_df_q1.count(), truncate=False)

        csv_output_path = os.path.join(base_data_path, "Q1_results")  # Path per il CSV
        # .coalesce(1) riduce il numero di partizioni a 1 per scrivere un singolo file CSV
        final_output_df_q1.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")
        print(f"Risultati Q1 salvati in CSV: {csv_output_path}")


    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()