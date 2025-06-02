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

def run_query3_spark_sql(spark_session, paths_to_read):
    start_time = time.time()

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {paths_to_read}")
    try:
        # Lettura dati Parquet specificando una lista di path
        df_processed = spark_session.read.parquet(*paths_to_read)

        if df_processed.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {paths_to_read}")
            spark_session.stop()
            exit()

    except Exception as e:
        print(f"Errore durante la lettura dei dati dalle partizioni: {e}")
        spark_session.stop()
        exit()

    df_processed.createOrReplaceTempView("q3_data_view")

    query_hourly_avg_sql = """
    SELECT
        country_code,
        hour,
        AVG(carbon_intensity) AS avg_carbon_intensity,
        AVG(carbon_free_percentage) AS avg_cfe
    FROM
        q3_data_view
    GROUP BY
        country_code, hour
    """

    hourly_avg_df_sql = spark_session.sql(query_hourly_avg_sql)

    hourly_avg_df_sql.createOrReplaceTempView("hourly_averages_q3_view")

    # Calcolo statistiche (min, percentili, max) usando la vista delle medie orarie
    query_final_stats_sql = """
    SELECT
        country_code,
        'carbon-intensity' AS data,
        MIN(avg_carbon_intensity) AS min_val,
        percentile(avg_carbon_intensity, 0.25) AS p25_val,
        percentile(avg_carbon_intensity, 0.50) AS p50_val,
        percentile(avg_carbon_intensity, 0.75) AS p75_val,
        MAX(avg_carbon_intensity) AS max_val
    FROM
        hourly_averages_q3_view
    GROUP BY
        country_code

    UNION ALL

    SELECT
        country_code,
        'cfe' AS data,
        MIN(avg_cfe) AS min_val,
        percentile_approx(avg_cfe, 0.25) AS p25_val,
        percentile_approx(avg_cfe, 0.50) AS p50_val,
        percentile_approx(avg_cfe, 0.75) AS p75_val,
        MAX(avg_cfe) AS max_val
    FROM
        hourly_averages_q3_view
    GROUP BY
        country_code
    """

    final_stats_df_q3_sql_intermediate = spark_session.sql(query_final_stats_sql)

    final_stats_df_q3_sql = final_stats_df_q3_sql_intermediate.select(
        F.col("country_code"),
        F.col("data"),
        F.col("min_val").alias("min"),
        F.col("p25_val").alias("25-perc"),
        F.col("p50_val").alias("50-perc"),
        F.col("p75_val").alias("75-perc"),
        F.col("max_val").alias("max")
    )

    # Azione per misurare il tempo sull'output finale delle statistiche
    final_stats_df_q3_sql.write.format("noop").mode("overwrite").save()

    end_time = time.time()

    return final_stats_df_q3_sql, end_time - start_time


def query3_sql(num_executor):
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query3") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", num_executor) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Path base ai dati processati e partizionati
    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read = [
        os.path.join(base_data_path, "country=Italy"),
        os.path.join(base_data_path, "country=Sweden")
    ]

    execution_times_sql = []
    output_df_q3_sql = None

    print(f"\nEsecuzione della Query Q3 con Spark SQL per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q3 SQL - Run {i + 1}/{N_RUN}")
        output_df_q3_sql, exec_time_sql = run_query3_spark_sql(spark, paths_to_read)
        execution_times_sql.append(exec_time_sql)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time_sql:.4f} secondi.")

    avg_time_sql = performance.print_performance(execution_times_sql, N_RUN, "Q3 Spark SQL")
    performance.log_performance_to_csv(spark, "Q3", "sql", avg_time_sql, num_executor)

    if output_df_q3_sql:
        print("\nRisultati finali per Q3 con Spark SQL:")
        output_df_q3_sql.show(n=output_df_q3_sql.count(), truncate=False)

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()





