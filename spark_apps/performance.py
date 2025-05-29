import os
import statistics

PERFORMANCE_CSV_PATH = "hdfs://namenode:8020/spark_data/spark/performance/"
HEADER_WRITTEN_FLAG_PERFORMANCE_CSV = False
INITIALIZED_QUERY_FILES = set()

def print_performance(execution_times, run, query):
    avg_time = 0
    if execution_times:
        # Elimino il primo elemento dalla lista
        execution_times = execution_times[1:]

        avg_time = statistics.mean(execution_times)
        print(f"\n--- Statistiche Tempi Esecuzione Query {query} ({run - 1} runs) ---")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times]}")
        print(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")
        if len(execution_times) > 1:  # La deviazione standard richiede almeno 2 campioni
            std_dev_time = statistics.stdev(execution_times)
            print(f"Deviazione standard dei tempi: {std_dev_time:.4f} secondi")
        print("----------------------------------------------------")
    return avg_time


"""def log_performance_to_csv(spark, query_name, query_type, avg_exec_time, num_executors, num_measured_runs):
    global HEADER_WRITTEN_FLAG_PERFORMANCE_CSV
    

    header = ["query_name", "query_type", "avg_execution_time_seconds", "num_executors", "num_runs"]
    row_data = [query_name, query_type, round(avg_exec_time, 4), num_executors, num_measured_runs]

    try:
        if not HEADER_WRITTEN_FLAG_PERFORMANCE_CSV:
            df_to_write = spark.createDataFrame([tuple(row_data)], schema=header)
            df_to_write.coalesce(1).write.csv(performance_path, mode="overwrite", header=True) # Sovrascrive la prima volta
            HEADER_WRITTEN_FLAG_PERFORMANCE_CSV = True
            print(f"Log delle performance medie salvato su: {performance_path}")
        else:
            df_to_write = spark.createDataFrame([tuple(row_data)], schema=header)
            df_to_write.coalesce(1).write.csv(PERFORMANCE_CSV_PATH, mode="append", header=False)
            print(f"Log delle performance medie salvato su: {PERFORMANCE_CSV_PATH}")

    except Exception as e:
        print(f"ERRORE: Impossibile scrivere il log delle performance medie su CSV: {e}")
        print(f"Dati che si tentava di scrivere: {row_data}")"""

def log_performance_to_csv(spark, query_name, query_type, avg_exec_time, num_executors, num_measured_runs):

    global INITIALIZED_QUERY_FILES

    try:
        os.makedirs(PERFORMANCE_CSV_PATH, exist_ok=True)
    except OSError as e:
        print(f"ERRORE: Impossibile creare la directory {PERFORMANCE_CSV_PATH}: {e}")
        return

    full_csv_path = os.path.join(PERFORMANCE_CSV_PATH, query_name)
    header_list = ["query_name", "query_type", "avg_execution_time_seconds", "num_executors", "num_runs"]
    row_data = [query_name, query_type, round(avg_exec_time, 4), num_executors, num_measured_runs]

    if num_executors == 1 and query_name not in INITIALIZED_QUERY_FILES:
        write_mode = "overwrite"
        write_header_flag = True
        INITIALIZED_QUERY_FILES.add(query_name)
    else:
        write_mode = "append"
        write_header_flag = False

    try:
        df_to_write = spark.createDataFrame([tuple(row_data)], schema=header_list)
        writer = df_to_write.coalesce(1).write
        writer.csv(full_csv_path, mode=write_mode, header=write_header_flag)
        print(
            f"Log delle performance per {query_name} ({query_type}) salvato su: {full_csv_path}")

    except Exception as e:
        print(f"ERRORE: Impossibile scrivere il log delle performance su CSV ({full_csv_path}): {e}")
        print(f"Dati che si tentava di scrivere: {row_data}")
