import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import statistics  # Per il calcolo della media e della dev.std

N_RUNS = 10  # Numero di run per l'esecuzione della query


# Funzione per la logica della query 1
def run_query1(spark_session, input_df):
    """
    Esegue la logica della Query 1: aggregazione annuale per paese.
    Calcola media, minimo e massimo di carbon_intensity e carbon_free_percentage.
    Restituisce il DataFrame risultato e il tempo di esecuzione della query.
    """
    query_start_time = time.time()  # Registra il tempo di inizio dell'esecuzione della query

    # Esegue l'aggregazione: raggruppa per 'year' e 'country_code'
    annual_aggregated_df = input_df.groupBy("year", "country_code") \
        .agg(  # Applica funzioni di aggregazione
        F.avg("carbon_intensity").alias("carbon_mean"),     # Media della carbon_intensity
        F.min("carbon_intensity").alias("carbon_min"),      # Minimo della carbon_intensity
        F.max("carbon_intensity").alias("carbon_max"),      # Massimo della carbon_intensity
        F.avg("carbon_free_percentage").alias("cfe_mean"),  # Media della cfe%
        F.min("carbon_free_percentage").alias("cfe_min"),   # Minimo della cfe%
        F.max("carbon_free_percentage").alias("cfe_max")    # Massimo della cfe%
    ) \
        .orderBy("country_code", "year")  # Ordina i risultati per paese e poi per anno

    # Rinomina la colonna 'year' in 'date'
    annual_aggregated_df_renamed = annual_aggregated_df.withColumnRenamed("year", "date")

    # Seleziona e ordina le colonne finali per l'output
    output_df_q1 = annual_aggregated_df_renamed.select(
        "date", "country_code",
        "carbon_mean", "carbon_min", "carbon_max",
        "cfe_mean", "cfe_min", "cfe_max"
    )

    # Azione Spark per forzare l'esecuzione di tutte le trasformazioni precedenti
    output_df_q1.collect()

    query_end_time = time.time()  # Registra il tempo di fine dell'esecuzione della query
    return output_df_q1, query_end_time - query_start_time  # Restituisce il DF e il tempo impiegato


if __name__ == "__main__":
    overall_start_time = time.time()  # Tempo di inizio per l'intero script

    # Inizializzazione della SparkSession
    spark = SparkSession.builder \
        .appName("ProjectSABD_Query1_Benchmark") \
        .getOrCreate()

    # Imposta il livello di logging di Spark a ERROR per ridurre la verbosità durante i benchmark
    spark.sparkContext.setLogLevel("ERROR")

    # Path base in HDFS dove si trovano i dati processati da preprocess_data.py
    base_processed_path = "hdfs://namenode:8020/spark_data/spark"

    # Specifica i percorsi delle partizioni per Italia (IT) e Svezia (SE)
    paths_to_read = [
        os.path.join(base_processed_path, "country=IT"),
        os.path.join(base_processed_path, "country=SE")
    ]

    print(f"Lettura dati iniziali da HDFS: {paths_to_read}")
    try:
        # Lettura dei file Parquet dai percorsi specificati
        df_processed = spark.read.parquet(*paths_to_read)

        # Mette in cache il DataFrame letto (i dati verranno effettivamente
        # messi in cache la prima volta che un'azione viene eseguita sul DataFrame)
        df_processed.cache()

        # Esegue un'azione count() per forzare la lettura dei dati da HDFS e la loro
        # materializzazione nella cache di Spark e ottiene anche il numero di righe
        initial_count = df_processed.count()
        if initial_count == 0:  # Controlla se sono stati letti dati
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {paths_to_read}")
            spark.stop()
            exit()
        print(f"Dati iniziali letti e messi in cache. Numero di righe: {initial_count}")
        # df_processed.printSchema() # stampa lo schema per verifica

    except Exception as e:
        print(f"Errore durante la lettura dei dati iniziali: {e}")
        spark.stop()
        exit()

    execution_times = []  # Lista per memorizzare i tempi di ogni esecuzione della query
    final_output_df_q1 = None  # Per salvare il risultato dell'ultima esecuzione

    print(f"\nEsecuzione della Query Q1 per {N_RUNS} volte...")
    for i in range(N_RUNS):
        print(f"Esecuzione Q1 - Run {i + 1}/{N_RUNS}")

        # Pulizia della cache di Spark per evitare caching nei run successivi
        spark.catalog.clearCache()

        # Esecuzione della query 1
        result_df, exec_time = run_query1(spark, df_processed)
        execution_times.append(exec_time)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
        if i == N_RUNS - 1:  # Se è l'ultima esecuzione, salva il DataFrame risultato
            final_output_df_q1 = result_df

    df_processed.unpersist()  # Rimuove df_processed dalla cache di Spark per liberare memoria

    # Calcola e stampa le statistiche dei tempi di esecuzione
    if execution_times:
        avg_time = statistics.mean(execution_times)
        print(f"\n--- Statistiche Tempi Esecuzione Query Q1 ({N_RUNS} runs) ---")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times]}")
        print(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")
        if len(execution_times) > 1:  # La deviazione standard richiede almeno 2 campioni
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard dei tempi: {std_dev_time:.4f} secondi")
        print("----------------------------------------------------")

    # Se c'è un DataFrame risultato dall'ultima esecuzione, va mostrato e salvato
    if final_output_df_q1:
        print("\nRisultati aggregati finali per Q1 (dall'ultima esecuzione):")
        # .count() in .show() è un'azione, quindi forza il calcolo se non già fatto
        final_output_df_q1.show(n=final_output_df_q1.count(), truncate=False)

        csv_output_path = os.path.join(base_processed_path, "Q1_results")  # Path per il CSV
        # .coalesce(1) riduce il numero di partizioni a 1 per scrivere un singolo file CSV
        final_output_df_q1.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")
        print(f"Risultati Q1 salvati in CSV: {csv_output_path}")

    overall_end_time = time.time()  # Tempo di fine per l'intero script
    print(
        f"\nTempo di esecuzione totale dello script (inclusi setup e I/O): {overall_end_time - overall_start_time:.2f} secondi")

    spark.stop()