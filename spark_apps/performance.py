import os
import statistics

PERFORMANCE_CSV_PATH = "hdfs://namenode:8020/spark_data/spark/performance/"

# Funzione per stampa delle performance
def print_performance(execution_times, run, query):
    avg_time = 0
    if execution_times:
        avg_time = statistics.mean(execution_times)
        print(f"\n--- Statistiche Tempi Esecuzione Query {query} ({run} runs) ---")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times]}")
        print(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")
        if len(execution_times) > 1:
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard dei tempi: {std_dev_time:.4f} secondi")
        print("----------------------------------------------------")
    return avg_time

# Funzione per salvataggio dei dati di performance su file CSV su HDFS
def log_performance_to_csv(spark, query_name, query_type, avg_exec_time, num_executors):

    try:
        os.makedirs(PERFORMANCE_CSV_PATH, exist_ok=True)
    except OSError as e:
        print(f"ERRORE: Impossibile creare la directory {PERFORMANCE_CSV_PATH}: {e}")
        return

    full_csv_path = os.path.join(PERFORMANCE_CSV_PATH, query_name, f"executor={num_executors}", query_type)
    header_list = ["query_name", "query_type", "avg_execution_time_seconds", "num_executors"]
    row_data = [query_name, query_type, round(avg_exec_time, 4), num_executors]
    try:
        df_to_write = spark.createDataFrame([tuple(row_data)], schema=header_list)
        writer = df_to_write.coalesce(1).write
        writer.csv(full_csv_path, mode="overwrite", header=True)
        print(f"Log delle performance per {query_name} ({query_type}) salvato su: {full_csv_path}")

    except Exception as e:
        print(f"ERRORE: Impossibile scrivere il log delle performance su CSV ({full_csv_path}): {e}")
        print(f"Dati che si tentava di scrivere: {row_data}")
