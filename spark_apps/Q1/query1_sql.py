import time
from pyspark.sql import SparkSession
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


N_RUN = 2

# Query Q1: Aggregare i dati su base annua
# Calcolare media, min, max di "Carbon intensity" e "Carbon-free energy percentage"
# per ciascun anno dal 2021 al 2024.


def run_query1_spark_sql(spark_session, paths_to_read):
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

    # Creazione di una vista temporanea per interrogazione SQL
    df_processed.createOrReplaceTempView("q1_data_view")

    query_q1_sql = """
    SELECT
        year AS date,
        country_code,
        AVG(carbon_intensity) AS carbon_mean,
        MIN(carbon_intensity) AS carbon_min,
        MAX(carbon_intensity) AS carbon_max,
        AVG(carbon_free_percentage) AS cfe_mean,
        MIN(carbon_free_percentage) AS cfe_min,
        MAX(carbon_free_percentage) AS cfe_max
    FROM
        q1_data_view
    GROUP BY
        year, country_code
    ORDER BY
        country_code, year
    """

    output_df_q1_sql = spark_session.sql(query_q1_sql)

    # Azione per forzare l'esecuzione e misurare il tempo
    output_df_q1_sql.write.format("noop").mode("overwrite").save()

    end_time = time.time()

    return output_df_q1_sql, end_time - start_time


if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "2") \
        .getOrCreate()

    sc = spark.sparkContext  # Ottieni SparkContext
    sc.setLogLevel("WARN")

    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read = [
        os.path.join(base_data_path, "country=Italy"),
        os.path.join(base_data_path, "country=Sweden")
    ]

    num_executors_active = spark.conf.get("spark.cores.max")
    print(f"Numero di executors {num_executors_active}")

    execution_times_sql = []
    output_df_q1_sql = None

    print(f"\nEsecuzione della Query Q1 con Spark SQL per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q1 SQL - Run {i + 1}/{N_RUN}")
        output_df_q1_sql, exec_time_sql = run_query1_spark_sql(spark, paths_to_read)
        execution_times_sql.append(exec_time_sql)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time_sql:.4f} secondi.")

    avg_time_sql = performance.print_performance(execution_times_sql, N_RUN, "Q1 Spark SQL")
    performance.log_performance_to_csv(spark, "Q1", "sql", avg_time_sql, num_executors_active)

    if output_df_q1_sql:
        print("\nRisultati finali per Q1 con Spark SQL:")
        output_df_q1_sql.show(n=output_df_q1_sql.count(), truncate=False)

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()