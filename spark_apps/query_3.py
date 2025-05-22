import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import statistics

N_RUNS = 10  # Numero di esecuzioni per la media


def run_query3_part1_logic(spark_session, hourly_avg_df_input):
    """Esegue la logica della Parte 1 della Query 3 (percentili) e restituisce il DataFrame e il tempo."""
    query_start_time = time.time()

    percentiles_to_calc = [0.0, 0.25, 0.50, 0.75, 1.0]
    percentile_names = ["min", "25_perc", "50_perc", "75_perc", "max"]

    ci_percentiles_df = hourly_avg_df_input.groupBy("country_code") \
        .agg(
        F.expr(
            f"percentile_approx(avg_hourly_carbon_intensity, array({','.join(map(str, percentiles_to_calc))}))").alias(
            "ci_values")
    ).withColumn("metric", F.lit("carbon-intensity")) \
        .select(
        F.col("country_code").alias("country"), "metric",
        F.col("ci_values")[0].alias(percentile_names[0]),
        F.col("ci_values")[1].alias(percentile_names[1]),
        F.col("ci_values")[2].alias(percentile_names[2]),
        F.col("ci_values")[3].alias(percentile_names[3]),
        F.col("ci_values")[4].alias(percentile_names[4])
    )

    cfe_percentiles_df = hourly_avg_df_input.groupBy("country_code") \
        .agg(
        F.expr(f"percentile_approx(avg_hourly_cfe, array({','.join(map(str, percentiles_to_calc))}))").alias(
            "cfe_values")
    ).withColumn("metric", F.lit("cfe")) \
        .select(
        F.col("country_code").alias("country"), "metric",
        F.col("cfe_values")[0].alias(percentile_names[0]),
        F.col("cfe_values")[1].alias(percentile_names[1]),
        F.col("cfe_values")[2].alias(percentile_names[2]),
        F.col("cfe_values")[3].alias(percentile_names[3]),
        F.col("cfe_values")[4].alias(percentile_names[4])
    )

    q3_percentiles_output_df = ci_percentiles_df.unionByName(cfe_percentiles_df).orderBy("country", "metric")
    q3_percentiles_output_df.collect()  # Azione per forzare il calcolo

    query_end_time = time.time()
    return q3_percentiles_output_df, query_end_time - query_start_time


def run_query3_part2_logic(spark_session, hourly_avg_df_input):
    """Esegue la logica della Parte 2 della Query 3 (dati per grafici) e restituisce il DataFrame e il tempo."""
    query_start_time = time.time()

    q3_hourly_graph_data_df = hourly_avg_df_input.select(
        F.col("country_code"),
        F.col("hour"),
        F.col("avg_hourly_carbon_intensity"),
        F.col("avg_hourly_cfe")
    ).orderBy("country_code", "hour")

    q3_hourly_graph_data_df.collect()  # Azione per forzare il calcolo

    query_end_time = time.time()
    return q3_hourly_graph_data_df, query_end_time - query_start_time


if __name__ == "__main__":
    overall_start_time = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query3_Benchmark") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    base_processed_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read_q3 = [
        os.path.join(base_processed_path, "country=IT"),
        os.path.join(base_processed_path, "country=SE")
    ]

    print(f"Lettura dati iniziali da HDFS per Q3: {paths_to_read_q3}")
    try:
        df_processed_q3 = spark.read.parquet(*paths_to_read_q3)
        df_processed_q3.cache()
        initial_count_q3 = df_processed_q3.count()
        if initial_count_q3 == 0:
            print(f"ERRORE: Nessun dato trovato nelle partizioni: {paths_to_read_q3}")
            spark.stop()
            exit()
        print(f"Dati letti e messi in cache per Q3. Numero di righe: {initial_count_q3}")
    except Exception as e:
        print(f"Errore durante la lettura dei dati iniziali per Q3: {e}")
        spark.stop()
        exit()

    # Calcola hourly_avg_df una volta e mettilo in cache,
    # se vuoi misurare le due parti della Q3 separatamente su questo df intermedio.
    print("\nCalcolo aggregazione oraria una tantum (hourly_avg_df) e caching...")
    hourly_avg_df = df_processed_q3.groupBy("country_code", "hour") \
        .agg(
        F.avg("carbon_intensity").alias("avg_hourly_carbon_intensity"),
        F.avg("carbon_free_percentage").alias("avg_hourly_cfe")
    )
    hourly_avg_df.cache()
    hourly_avg_df.count()  # Materializza
    print("Dati orari medi (hourly_avg_df) calcolati e messi in cache.")

    # Benchmark Parte 1: Percentili
    execution_times_q3_p1 = []
    final_q3_percentiles_df = None
    print(f"\nEsecuzione Query Q3 - Parte 1 (Percentili) per {N_RUNS} volte...")
    for i in range(N_RUNS):
        print(f"Esecuzione Q3-P1 - Run {i + 1}/{N_RUNS}")
        spark.catalog.clearCache()  # Vedi nota su clearCache nel q1

        result_df, exec_time = run_query3_part1_logic(spark, hourly_avg_df)
        execution_times_q3_p1.append(exec_time)
        print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
        if i == N_RUNS - 1:
            final_q3_percentiles_df = result_df

    if execution_times_q3_p1:
        avg_time_q3_p1 = statistics.mean(execution_times_q3_p1)
        print(f"\n--- Statistiche Tempi Esecuzione Q3-P1 ({N_RUNS} runs) ---")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times_q3_p1]}")
        print(f"Tempo medio di esecuzione: {avg_time_q3_p1:.4f} secondi")
        if len(execution_times_q3_p1) > 1:
            std_dev_time_q3_p1 = statistics.stdev(execution_times_q3_p1)
            print(f"Deviazione standard dei tempi: {std_dev_time_q3_p1:.4f} secondi")
        print("-------------------------------------------------------")

    if final_q3_percentiles_df:
        print("\nRisultati percentili Q3 finali (dall'ultima esecuzione):")
        final_q3_percentiles_df.show(truncate=False)
        csv_output_path_percentiles = os.path.join(base_processed_path, "Q3_percentiles_results")
        final_q3_percentiles_df.coalesce(1).write.csv(csv_output_path_percentiles, header=True, mode="overwrite")
        print(f"Risultati percentili Q3 salvati in CSV: {csv_output_path_percentiles}")

    # Benchmark Parte 2: Dati per Grafici
    execution_times_q3_p2 = []
    final_q3_graph_data_df = None
    print(f"\nEsecuzione Query Q3 - Parte 2 (Dati Grafici) per {N_RUNS} volte...")
    for i in range(N_RUNS):
        print(f"Esecuzione Q3-P2 - Run {i + 1}/{N_RUNS}")
        spark.catalog.clearCache()

        result_df, exec_time = run_query3_part2_logic(spark, hourly_avg_df)  # Usa lo stesso hourly_avg_df cachato
        execution_times_q3_p2.append(exec_time)
        print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
        if i == N_RUNS - 1:
            final_q3_graph_data_df = result_df

    hourly_avg_df.unpersist()  # Rimuovi hourly_avg_df dalla cache
    df_processed_q3.unpersist()  # Rimuovi il DataFrame iniziale Q3 dalla cache

    if execution_times_q3_p2:
        avg_time_q3_p2 = statistics.mean(execution_times_q3_p2)
        print(f"\n--- Statistiche Tempi Esecuzione Q3-P2 ({N_RUNS} runs) ---")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times_q3_p2]}")
        print(f"Tempo medio di esecuzione: {avg_time_q3_p2:.4f} secondi")
        if len(execution_times_q3_p2) > 1:
            std_dev_time_q3_p2 = statistics.stdev(execution_times_q3_p2)
            print(f"Deviazione standard dei tempi: {std_dev_time_q3_p2:.4f} secondi")
        print("-------------------------------------------------------")

    if final_q3_graph_data_df:
        print("\nDati orari aggregati finali per grafici Q3 (dall'ultima esecuzione):")
        final_q3_graph_data_df.show(final_q3_graph_data_df.count(), truncate=False)
        csv_output_path_hourly_graph = os.path.join(base_processed_path, "Q3_hourly_graph_data_results")
        final_q3_graph_data_df.coalesce(1).write.csv(csv_output_path_hourly_graph, header=True, mode="overwrite")
        print(f"Dati per grafici orari Q3 salvati in CSV: {csv_output_path_hourly_graph}")

    overall_end_time = time.time()
    print(
        f"\nTempo di esecuzione totale dello script Q3 (inclusi setup e I/O): {overall_end_time - overall_start_time:.2f} secondi")

    spark.stop()