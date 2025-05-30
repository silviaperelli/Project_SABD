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


N_RUN = 11

def format_for_output(df_input):
    return df_input.select(
        F.col("date"),
        F.col("avg_carbon_intensity").alias("carbon_intensity"),
        F.col("avg_cfe").alias("cfe")
    )

# Query Q2: Aggregare i dati sulla coppia (anno, mese)
# Calcolare media "Carbon intensity" e "Carbon-free energy percentage" per coppia (anno, mese)
# Calcolare la classifica delle prime 5 coppie ordinando per “Carbon intensity” decrescente, crescente e
# “Carbon-free energy percentage” decrescente, crescente.

def run_query2_spark_sql(spark_session, df_processed):
    start_time = time.time()

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {path_to_read}")
    try:
        # Lettura dei dati Parquet specificando una lista di path
        df_processed = spark_session.read.parquet(path_to_read)

        if df_processed.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {path_to_read}")
            spark_session.stop()
            exit()

    except Exception as e:
        print(f"Errore durante la lettura dei dati dalle partizioni: {e}")
        spark_session.stop()
        exit()

    df_processed.createOrReplaceTempView("q2_data_view")

    query_monthly_aggregates_cte = """
    WITH MonthlyAggregates AS (
        SELECT
            year,
            month,
            CONCAT(year, '_', LPAD(month, 2, '0')) AS date,
            AVG(carbon_intensity) AS avg_carbon_intensity,
            AVG(carbon_free_percentage) AS avg_cfe
        FROM
            q2_data_view
        GROUP BY
            year, month
    )
    """

    query_q2_top_sql = query_monthly_aggregates_cte + """
    (SELECT date, avg_carbon_intensity AS carbon_intensity, avg_cfe AS cfe FROM MonthlyAggregates ORDER BY avg_carbon_intensity DESC LIMIT 5)
    UNION ALL
    (SELECT date, avg_carbon_intensity AS carbon_intensity, avg_cfe AS cfe FROM MonthlyAggregates ORDER BY avg_carbon_intensity ASC LIMIT 5)
    UNION ALL
    (SELECT date, avg_carbon_intensity AS carbon_intensity, avg_cfe AS cfe FROM MonthlyAggregates ORDER BY avg_cfe DESC LIMIT 5)
    UNION ALL
    (SELECT date, avg_carbon_intensity AS carbon_intensity, avg_cfe AS cfe FROM MonthlyAggregates ORDER BY avg_cfe ASC LIMIT 5)
    """
    final_df_q2_sql = spark_session.sql(query_q2_top_sql)

    # Azione per forzare l'esecuzione della query dei top 5
    final_df_q2_sql.write.format("noop").mode("overwrite").save()

    end_time = time.time()
    return final_df_q2_sql, end_time - start_time


def query2_sql(num_executor):
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query2") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Path base ai dati processati e partizionati
    base_data_path = "hdfs://namenode:8020/spark_data/spark"

    # Path specifico per i dati dell'Italia
    path_to_read = os.path.join(base_data_path, "country=Italy")

    execution_times_sql = []
    output_df_q2_sql = None

    num_executors_active = spark.conf.get("spark.cores.max")
    print(f"Numero di executors {num_executors_active}")

    print(f"\nEsecuzione della Query Q2 con Spark SQL per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q2 SQL - Run {i + 1}/{N_RUN}")
        output_df_q2_sql, exec_time_sql = run_query2_spark_sql(spark, path_to_read)
        execution_times_sql.append(exec_time_sql)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time_sql:.4f} secondi.")

    avg_time_sql = performance.print_performance(execution_times_sql, N_RUN, "Q2 Spark SQL")
    performance.log_performance_to_csv(spark, "Q2", "sql", avg_time_sql, num_executors_active)

    if output_df_q2_sql:
        print("\nRisultati finali per Q2 con Spark SQL:")
        output_df_q2_sql.show(n=output_df_q2_sql.count(), truncate=False)

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()





