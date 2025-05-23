import statistics
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

N_RUN = 10

def format_for_output(df_input):
    return df_input.select(
        F.col("date_str").alias("date"),
        F.col("avg_carbon_intensity").alias("carbon_intensity"),
        F.col("avg_cfe").alias("cfe")
    )

# Query Q2: Aggregare i dati sulla coppia (anno, mese)
# Calcolare media "Carbon intensity" e "Carbon-free energy percentage" per coppia (anno, mese)
# Calcolare la classifica delle prime 5 coppie ordinando per “Carbon intensity” decrescente, crescente e
# “Carbon-free energy percentage” decrescente, crescente.

def run_query2(spark_session, path_to_read):
    start_time = time.time()

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {path_to_read}")
    try:
        # Leggi i dati Parquet specificando una lista di path
        df_processed = spark_session.read.parquet(path_to_read)

        if df_processed.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {path_to_read}")
            spark_session.stop()
            exit()

    except Exception as e:
        print(f"Errore durante la lettura dei dati dalle partizioni: {e}")
        spark_session.stop()
        exit()

    print("\nEsecuzione della Query Q2...")

    # Aggregazione per (anno, mese)
    monthly_aggregated_it_df = df_processed.groupBy("year", "month") \
        .agg(
        F.avg("carbon_intensity").alias("avg_carbon_intensity"),
        F.avg("carbon_free_percentage").alias("avg_cfe")) \
        .withColumn("date_str", F.concat(F.col("year"), F.lit("_"), F.lpad(F.col("month"), 2, '0'))) \
        .select("date_str", "avg_carbon_intensity", "avg_cfe", "year", "month")  # Mantenere year e month per ordinamenti

    monthly_aggregated_it_df.cache()

    # 1. Carbon intensity decrescente (peggiori)
    ci_desc = monthly_aggregated_it_df.orderBy(F.col("avg_carbon_intensity").desc()) \
        .limit(5)
    ci_desc_output = format_for_output(ci_desc)

    # 2. Carbon intensity crescente (migliori)
    ci_asc = monthly_aggregated_it_df.orderBy(F.col("avg_carbon_intensity").asc()) \
        .limit(5)
    ci_asc_output = format_for_output(ci_asc)

    # 3. Carbon-free energy percentage decrescente (migliori)
    cfe_desc = monthly_aggregated_it_df.orderBy(F.col("avg_cfe").desc()) \
        .limit(5)
    cfe_desc_output = format_for_output(cfe_desc)

    # 4. Carbon-free energy percentage crescente (peggiori)
    cfe_asc = monthly_aggregated_it_df.orderBy(F.col("avg_cfe").asc()) \
        .limit(5)
    cfe_asc_output = format_for_output(cfe_asc)

    # Unione di tutti i DataFrame per l'output unico
    final_df_q2 = ci_desc_output \
        .unionAll(ci_asc_output) \
        .unionAll(cfe_desc_output) \
        .unionAll(cfe_asc_output)

    monthly_aggregated_it_df.unpersist()  # Rimuovi dalla cache

    final_df_q2.write.format("noop").mode("overwrite").save()

    end_time = time.time()

    return final_df_q2, monthly_aggregated_it_df, end_time - start_time


if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Path base ai dati processati e partizionati
    base_data_path = "hdfs://namenode:8020/spark_data/spark"

    # Path specifico per i dati dell'Italia
    path_to_read = os.path.join(base_data_path, "country=Italy")

    execution_times = []  # Lista per memorizzare i tempi di ogni esecuzione della query
    final_output_df_q2 = None  # Per salvare il risultato dell'ultima esecuzione
    final_monthly_df = None # Per salvare il dataframe aggregato su coppia (anno, mese)

    print(f"\nEsecuzione della Query Q2 per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"Esecuzione Q2 - Run {i + 1}/{N_RUN}")

        result_df, monthly_df, exec_time = run_query2(spark, path_to_read)
        execution_times.append(exec_time)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
        if i == N_RUN - 1:  # Se è l'ultima esecuzione, salva il DataFrame risultato
            final_output_df_q2 = result_df
            final_monthly_df = monthly_df


    # Calcola e stampa le statistiche dei tempi di esecuzione
    if execution_times:
        avg_time = statistics.mean(execution_times)
        print(f"\n--- Statistiche Tempi Esecuzione Query Q2 ({N_RUN} runs) ---")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times]}")
        print(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")
        if len(execution_times) > 1:  # La deviazione standard richiede almeno 2 campioni
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard dei tempi: {std_dev_time:.4f} secondi")
        print("----------------------------------------------------")

    if final_output_df_q2 and final_monthly_df:
        print("\nRisultati aggregati finali per Q2:")

        final_output_df_q2.show(n=final_output_df_q2.count(), truncate=False)

        csv_output_path = os.path.join(base_data_path, "Q2_results")  # Path per il CSV
        csv_graphs_path = os.path.join(base_data_path, "Q2_graphs") # Path per il CSV per i grafici
        # .coalesce(1) riduce il numero di partizioni a 1 per scrivere un singolo file CSV
        final_output_df_q2.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")
        final_monthly_df.coalesce(1).write.csv(csv_graphs_path, header=True, mode="overwrite")
        print(f"Risultati Q2 salvati in CSV: {csv_output_path} e in {csv_graphs_path}")

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()





