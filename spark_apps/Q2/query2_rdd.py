import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
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

FINAL_Q2_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("carbon_intensity", DoubleType(), True),
    StructField("cfe", DoubleType(), True)
])


# Query Q2: Aggregare i dati sulla coppia (anno, mese)
# Calcolare media "Carbon intensity" e "Carbon-free energy percentage" per coppia (anno, mese)
# Calcolare la classifica delle prime 5 coppie ordinando per “Carbon intensity” decrescente, crescente e
# “Carbon-free energy percentage” decrescente, crescente.

def run_query2_rdd(spark_session, path_to_read):
    start_time = time.time()

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {path_to_read}")
    try:
        # Lettura dati Parquet specificando una lista di path
        df_processed = spark_session.read.parquet(path_to_read)

        if df_processed.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {path_to_read}")
            spark_session.stop()
            exit()

        input_rdd = df_processed.rdd

    except Exception as e:
        print(f"Errore durante la lettura dei dati dalle partizioni: {e}")
        spark_session.stop()
        exit()


    mapped_rdd = input_rdd.map(lambda row: (
        (row['year'], row['month']),
        (row['carbon_intensity'], row['carbon_free_percentage'], 1)
    ))

    # Riduci per chiave
    def reduce_monthly_aggregates(a, b):
        # a, b sono (sum_ci, sum_cfe, count_cfe)
        return (a[0] + b[0], a[1] + b[1], a[2] + b[2])

    reduced_rdd = mapped_rdd.reduceByKey(reduce_monthly_aggregates)

    # Calcola medie e formatta la data
    def calculate_averages_and_format_date_q2(item):
        key, value = item
        year, month = key
        sum_ci, sum_cfe, count = value

        # Formatta la data come YYYY_MM
        date_str = f"{year}_{str(month).zfill(2)}"

        avg_ci = sum_ci / count if count > 0 else None
        avg_cfe = sum_cfe / count if count > 0 else None

        return Row(
            date=date_str,
            year=year,
            month=month,
            avg_carbon_intensity=avg_ci,
            avg_cfe=avg_cfe
        )

    monthly_aggregated_rdd = reduced_rdd.map(calculate_averages_and_format_date_q2)
    monthly_aggregated_rdd.cache()  # Cache perché verrà usato più volte

    # --- Operazioni di Top/Bottom 5 ---
    # 1. Carbon intensity decrescente (peggiori)
    ci_desc_list = monthly_aggregated_rdd.filter(lambda r: r.avg_carbon_intensity is not None) \
        .sortBy(lambda r: r.avg_carbon_intensity, ascending=False) \
        .take(5)
    ci_desc_output_rows = [Row(date=r.date, carbon_intensity=r.avg_carbon_intensity, cfe=r.avg_cfe) for r in ci_desc_list]

    # 2. Carbon intensity crescente (migliori)
    ci_asc_list = monthly_aggregated_rdd.filter(lambda r: r.avg_carbon_intensity is not None) \
        .sortBy(lambda r: r.avg_carbon_intensity, ascending=True) \
        .take(5)
    ci_asc_output_rows = [Row(date=r.date, carbon_intensity=r.avg_carbon_intensity, cfe=r.avg_cfe) for r in ci_asc_list]

    # 3. Carbon-free energy percentage decrescente (migliori)
    cfe_desc_list = monthly_aggregated_rdd.filter(lambda r: r.avg_cfe is not None) \
        .sortBy(lambda r: r.avg_cfe, ascending=False) \
        .take(5)
    cfe_desc_output_rows = [Row(date=r.date, carbon_intensity=r.avg_carbon_intensity, cfe=r.avg_cfe) for r in cfe_desc_list]

    # 4. Carbon-free energy percentage crescente (peggiori)
    cfe_asc_list = monthly_aggregated_rdd.filter(lambda r: r.avg_cfe is not None) \
        .sortBy(lambda r: r.avg_cfe, ascending=True) \
        .take(5)
    cfe_asc_output_rows = [Row(date=r.date, carbon_intensity=r.avg_carbon_intensity, cfe=r.avg_cfe) for r in cfe_asc_list]
    monthly_aggregated_rdd.unpersist()  # Rimuovi dalla cache

    # Unione di tutte le liste di Row
    all_output_rows = ci_desc_output_rows + ci_asc_output_rows + cfe_desc_output_rows + cfe_asc_output_rows

    # Creazione del DataFrame finale
    # Se all_output_rows è vuota, crea un DataFrame vuoto con lo schema
    if not all_output_rows:
        final_df_q2_rdd = spark_session.createDataFrame([], schema=FINAL_Q2_SCHEMA)
    else:
        final_df_q2_rdd = spark_session.createDataFrame(all_output_rows, schema=FINAL_Q2_SCHEMA)

    final_df_q2_rdd.write.format("noop").mode("overwrite").save()

    end_time = time.time()
    exec_time = end_time - start_time

    return final_df_q2_rdd, exec_time

def query2_rdd(num_executor):
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query2") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", num_executor) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Path base ai dati processati e partizionati
    base_data_path = "hdfs://namenode:8020/spark_data/spark"

    # Path specifico per i dati dell'Italia
    path_to_read = os.path.join(base_data_path, "country=Italy")

    execution_times_rdd = []
    output_df_q2_rdd = None

    print(f"\nEsecuzione della Query Q2 con RDD per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q2 con RDD - Run {i + 1}/{N_RUN}")
        output_df_q2_rdd, exec_time_rdd = run_query2_rdd(spark, path_to_read)
        execution_times_rdd.append(exec_time_rdd)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time_rdd:.4f} secondi.")

    avg_time_rdd = performance.print_performance(execution_times_rdd, N_RUN, "Q2 Spark RDD")
    performance.log_performance_to_csv(spark, "Q2", "rdd", avg_time_rdd, num_executor)

    if output_df_q2_rdd:
        print("\nRisultati finali per Q2 con RDD:")
        output_df_q2_rdd.show(n=output_df_q2_rdd.count(), truncate=False)

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()





