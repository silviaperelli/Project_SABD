import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import statistics # Per il calcolo della media e della dev.std

N_RUNS = 10 # Numero di run per l'esecuzione della query


"""Formatta il DataFrame per l'output finale della Q2, selezionando e rinominando le colonne."""
def format_for_q2_output(df_input):
    return df_input.select(
        F.col("date_str").alias("date"),
        F.col("avg_carbon_intensity").alias("carbon_intensity"),
        F.col("avg_cfe").alias("cfe")
    )

"""
    Esegue la logica principale della Query 2: calcola le 4 classifiche top 5.
    Prende in input il DataFrame mensile aggregato (che dovrebbe essere cachato).
    Restituisce i 4 DataFrame risultato e il tempo di esecuzione.
    """
def run_query2_core_logic(spark_session, monthly_aggregated_df_input):

    query_start_time = time.time() # Registra il tempo di inizio dell'esecuzione della query

    # 1. Carbon intensity decrescente (peggiori)
    ci_desc = monthly_aggregated_df_input.orderBy(F.col("avg_carbon_intensity").desc()).limit(5)
    ci_desc_output = format_for_q2_output(ci_desc)
    ci_desc_output.collect()

    # 2. Carbon intensity crescente (migliori)
    ci_asc = monthly_aggregated_df_input.orderBy(F.col("avg_carbon_intensity").asc()).limit(5)
    ci_asc_output = format_for_q2_output(ci_asc)
    ci_asc_output.collect()

    # 3. Carbon-free energy percentage decrescente (migliori)
    cfe_desc = monthly_aggregated_df_input.orderBy(F.col("avg_cfe").desc()).limit(5)
    cfe_desc_output = format_for_q2_output(cfe_desc)
    cfe_desc_output.collect()

    # 4. Carbon-free energy percentage crescente (peggiori)
    cfe_asc = monthly_aggregated_df_input.orderBy(F.col("avg_cfe").asc()).limit(5)
    cfe_asc_output = format_for_q2_output(cfe_asc)
    cfe_asc_output.collect()

    query_end_time = time.time() # Registra il tempo di fine dell'esecuzione della query
    return ci_desc_output, ci_asc_output, cfe_desc_output, cfe_asc_output, query_end_time - query_start_time


if __name__ == "__main__":
    overall_start_time = time.time()

    # Inizializzazione della SparkSession
    spark = SparkSession.builder \
        .appName("ProjectSABD_Query2_Benchmark") \
        .getOrCreate()

    # Imposta il livello di logging di Spark a ERROR per ridurre la verbosità durante i benchmark
    spark.sparkContext.setLogLevel("ERROR")

    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read_q2 = os.path.join(base_data_path, "country=IT")

    print(f"Lettura dati iniziali da HDFS per Q2 (solo IT): {paths_to_read_q2}")
    try:
        # Lettura dei file Parquet dai percorsi specificati
        df_processed_q2 = spark.read.parquet(paths_to_read_q2)
        df_processed_q2.cache()  # Cache del DataFrame letto da HDFS
        initial_count_q2 = df_processed_q2.count()  # Azione per materializzare la cache
        if initial_count_q2 == 0:
            print(f"ERRORE: Nessun dato trovato nella partizione IT: {paths_to_read_q2}")
            spark.stop()
            exit()
        print(f"Dati IT letti e messi in cache. Numero di righe: {initial_count_q2}")
        # df_processed_q2.printSchema()
    except Exception as e:
        print(f"Errore durante la lettura dei dati iniziali per Q2: {e}")
        spark.stop()
        exit()

    # Calcola l'aggregazione mensile una volta e la mette in cache
    # La misurazione dei tempi si concentrerà sulle operazioni di ordinamento e limitazione
    print("\nCalcolo aggregazione mensile (monthly_aggregated_it_df) e caching...")
    monthly_aggregated_it_df = df_processed_q2.groupBy("year", "month") \
        .agg(
        F.avg("carbon_intensity").alias("avg_carbon_intensity"),
        F.avg("carbon_free_percentage").alias("avg_cfe")
    ) \
        .withColumn("date_str", F.concat(F.col("year"), F.lit("_"), F.lpad(F.col("month"), 2, '0'))) \
        .select("date_str", "avg_carbon_intensity", "avg_cfe", "year", "month")

    monthly_aggregated_it_df.cache()
    monthly_aggregated_it_df.count()  # Materializza monthly_aggregated_it_df in cache
    print("Dati mensili aggregati e messi in cache.")

    execution_times_q2 = []
    # Variabili per salvare i risultati dell'ultima esecuzione
    last_run_ci_desc, last_run_ci_asc, last_run_cfe_desc, last_run_cfe_asc = None, None, None, None

    print(f"\nEsecuzione della logica principale della Query Q2 per {N_RUNS} volte...")
    for i in range(N_RUNS):
        print(f"Esecuzione Q2 - Run {i + 1}/{N_RUNS}")

        # Opzionale: Pulire la cache di Spark
        # spark.catalog.clearCache()

        r_ci_desc, r_ci_asc, r_cfe_desc, r_cfe_asc, exec_time = run_query2_core_logic(spark, monthly_aggregated_it_df)
        execution_times_q2.append(exec_time)
        print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
        if i == N_RUNS - 1:  # Salva i risultati dell'ultima esecuzione
            last_run_ci_desc, last_run_ci_asc, last_run_cfe_desc, last_run_cfe_asc = r_ci_desc, r_ci_asc, r_cfe_desc, r_cfe_asc

    df_processed_q2.unpersist()  # Rimozione di df sorgente dalla cache
    monthly_aggregated_it_df.unpersist()  # Rimozione di df aggregato dalla cache

    # Calcolo e stampa statistiche dei tempi
    if execution_times_q2:
        avg_time_q2 = statistics.mean(execution_times_q2)
        print(f"\n--- Statistiche Tempi Esecuzione Logica Q2 ({N_RUNS} runs) ---")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times_q2]}")
        print(f"Tempo medio di esecuzione: {avg_time_q2:.4f} secondi")
        if len(execution_times_q2) > 1:
            std_dev_time_q2 = statistics.stdev(execution_times_q2)
            print(f"Deviazione standard dei tempi: {std_dev_time_q2:.4f} secondi")
        print("-------------------------------------------------------")

    # Salva i risultati dell'ultima esecuzione e li mostra
    if last_run_ci_desc:
        print("\nRisultati Q2 (dall'ultima esecuzione):")
        print("Top 5 (anno, mese) per Carbon Intensity (decrescente - peggiori):")
        last_run_ci_desc.show(truncate=False)
        print("Top 5 (anno, mese) per Carbon Intensity (crescente - migliori):")
        last_run_ci_asc.show(truncate=False)
        print("Top 5 (anno, mese) per Carbon-Free Energy % (decrescente - migliori):")
        last_run_cfe_desc.show(truncate=False)
        print("Top 5 (anno, mese) per Carbon-Free Energy % (crescente - peggiori):")
        last_run_cfe_asc.show(truncate=False)

        final_df_q2_to_save = last_run_ci_desc \
            .unionByName(last_run_ci_asc) \
            .unionByName(last_run_cfe_desc) \
            .unionByName(last_run_cfe_asc)

        csv_output_path_q2 = os.path.join(base_data_path, "Q2_results")
        final_df_q2_to_save.coalesce(1).write.csv(csv_output_path_q2, header=True, mode="overwrite")
        print(f"Risultati Q2 salvati in CSV: {csv_output_path_q2}")

    overall_end_time = time.time()
    print(
        f"\nTempo di esecuzione totale dello script Q2 (inclusi setup e I/O): {overall_end_time - overall_start_time:.2f} secondi")

    spark.stop()