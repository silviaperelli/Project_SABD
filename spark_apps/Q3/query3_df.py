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

# Query Q3: Aggregare i dati sulle 24 ore
# Aggregare i dati di ciascun paese sulle 24 ore della giornata, calcolando il valor medio di “Carbon intensity gCO2eq/kWh (direct)”
# e “Carbon-free energy percentage (CFE%)”. Calcolare il minimo, 25-esimo, 50-esimo, 75-esimo percentile
# e massimo del valor medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbon-free energy percentage (CFE%)”.

# Funzione per il calcolo del percentile e dei valori minimi e massimi
def calculate_percentiles_and_stats(df_input, value_col_name, metric_name_output):

    percentiles_df = df_input.groupBy("country_code") \
        .agg(
            F.lit(metric_name_output).alias("data"),
            F.min(F.col(value_col_name)).alias("min_val"),
            F.expr(f"percentile({value_col_name}, 0.25)").alias("p25_val"),
            F.expr(f"percentile({value_col_name}, 0.50)").alias("p50_val"),
            F.expr(f"percentile({value_col_name}, 0.75)").alias("p75_val"),
            F.max(F.col(value_col_name)).alias("max_val")
        ).select(
            "country_code",
            "data",
            F.col("min_val").alias("min"),
            F.col("p25_val").alias("25-perc"),
            F.col("p50_val").alias("50-perc"),
            F.col("p75_val").alias("75-perc"),
            F.col("max_val").alias("max")
        )
    return percentiles_df


def run_query3(spark_session, paths_to_read):
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

    # Aggregazione dati per paese e ora della giornata
    hourly_avg_df = df_processed.groupBy("country_code", "hour") \
        .agg(
            F.avg("carbon_intensity").alias("avg_carbon_intensity"),
            F.avg("carbon_free_percentage").alias("avg_cfe")
        )

    hourly_avg_df.cache() # Memorizzazione in cache

    # Calcolo min, 25, 50, 75 percentile e max per "carbon_intensity"
    carbon_intensity_stats_df = calculate_percentiles_and_stats(hourly_avg_df,"avg_carbon_intensity","carbon-intensity")

    # Calcolo min, 25, 50, 75 percentile e max per "carbon_free_percentage"
    cfe_stats_df = calculate_percentiles_and_stats(hourly_avg_df,"avg_cfe","cfe")

    # Unione dei risultati in unico DataFrame
    final_stats_df_q3 = carbon_intensity_stats_df.unionByName(cfe_stats_df)

    # Azione per forzare l'esecuzione e misurare il tempo
    final_stats_df_q3.write.format("noop").mode("overwrite").save()

    # Rimozione dalla cache
    hourly_avg_df.unpersist()

    end_time = time.time()

    return final_stats_df_q3, hourly_avg_df, end_time - start_time

def query3_df(num_executor):
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query3") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", num_executor) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read = [
        os.path.join(base_data_path, "country=Italy"),
        os.path.join(base_data_path, "country=Sweden")
    ]

    execution_times = []  # Lista per memorizzare i tempi di ogni esecuzione della query
    final_output_df_q3 = None  # Per salvare il risultato dell'ultima esecuzione
    final_hourly_df = None # Per salvare il dataframe aggregato sulle 24 ore

    print(f"\nEsecuzione della Query Q3 con DataFrame per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q3 DataFrame - Run {i + 1}/{N_RUN}")

        result_df, hourly_df, exec_time = run_query3(spark, paths_to_read)
        execution_times.append(exec_time)
        print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
        if i == N_RUN - 1:
            final_output_df_q3 = result_df
            final_hourly_df = hourly_df

    avg_time = performance.print_performance(execution_times, N_RUN, "Q3")
    performance.log_performance_to_csv(spark, "Q3", "dataframe", avg_time, num_executor)

    if final_output_df_q3 and final_hourly_df:
        print("\nRisultati aggregati finali per Q3 con DataFrame:")

        final_output_df_q3.orderBy("country_code").show(n=final_output_df_q3.count(), truncate=False)

        csv_output_path = os.path.join(base_data_path, "Q3_results")
        csv_graphs_path = os.path.join(base_data_path, "Q3_graphs")
        # .coalesce(1) riduce il numero di partizioni a 1 per scrivere un singolo file CSV
        final_output_df_q3.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")
        final_hourly_df.coalesce(1).write.csv(csv_graphs_path, header=True, mode="overwrite")
        print(f"Risultati Q3 salvati in CSV: {csv_output_path} e {csv_graphs_path}")


    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()





