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


FINAL_Q3_SCHEMA = StructType([
    StructField("country_code", StringType(), True),
    StructField("data", StringType(), True),  # "carbon-intensity" o "cfe"
    StructField("min", DoubleType(), True),
    StructField("25-perc", DoubleType(), True),
    StructField("50-perc", DoubleType(), True),
    StructField("75-perc", DoubleType(), True),
    StructField("max", DoubleType(), True)
])


def _calculate_percentiles_from_list(values_list):

    # Calcola min, p25, p50, p75, max da una lista di numeri.
    # Usa il metodo "nearest rank" semplice per i percentili.

    valid_values = sorted([float(v) for v in values_list if v is not None])
    n = len(valid_values)

    min_val = valid_values[0]
    max_val = valid_values[-1]

    if n == 1:
        return min_val, min_val, min_val, min_val, max_val  # Tutti i percentili sono il valore stesso

    idx_p25 = int(round(0.25 * (n - 1)))
    idx_p50 = int(round(0.50 * (n - 1)))
    idx_p75 = int(round(0.75 * (n - 1)))

    idx_p25 = max(0, min(idx_p25, n - 1))
    idx_p50 = max(0, min(idx_p50, n - 1))
    idx_p75 = max(0, min(idx_p75, n - 1))

    p25_val = valid_values[idx_p25]
    p50_val = valid_values[idx_p50]
    p75_val = valid_values[idx_p75]

    return min_val, p25_val, p50_val, p75_val, max_val


def run_query3_rdd(spark_session, paths_to_read):
    start_time_func = time.time()

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {paths_to_read}")
    try:
        # Lettura dati Parquet specificando una lista di path
        df_processed = spark_session.read.parquet(*paths_to_read)

        if df_processed.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {paths_to_read}")
            spark_session.stop()
            exit()

        input_rdd = df_processed.rdd

    except Exception as e:
        print(f"Errore durante la lettura dei dati dalle partizioni: {e}")
        spark_session.stop()
        exit()


    mapped_hourly_rdd = input_rdd.map(lambda row: (
        (row['hour'], row['country_code']),
        (row['carbon_intensity'], row['carbon_free_percentage'], 1)
    ))

    def reduce_hourly_aggregates(a, b):
        # a, b sono (sum_ci, count_ci, sum_cfe, count_cfe)
        return (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3])

    reduced_hourly_rdd = mapped_hourly_rdd.reduceByKey(reduce_hourly_aggregates)

    def calculate_hourly_averages(item):
        key, value = item
        country_code, hour = key
        sum_ci, count_ci, sum_cfe, count_cfe = value

        avg_ci = sum_ci / count_ci if count_ci > 0 else None
        avg_cfe = sum_cfe / count_cfe if count_cfe > 0 else None

        return Row(country_code=country_code, hour=hour, avg_carbon_intensity=avg_ci, avg_cfe=avg_cfe)

    hourly_avg_rdd = reduced_hourly_rdd.map(calculate_hourly_averages)
    hourly_avg_rdd.cache()

    # Controllo se hourly_avg_rdd è vuoto
    if hourly_avg_rdd.isEmpty():
        print("Nessun dato aggregato orario prodotto (RDD vuoto).")
        hourly_avg_rdd.unpersist()
        empty_final_df = spark_session.createDataFrame([], FINAL_Q3_SCHEMA)
        empty_hourly_df = spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(), StructType([
            StructField("country_code", StringType()), StructField("hour", IntegerType()),
            StructField("avg_carbon_intensity", DoubleType()), StructField("avg_cfe", DoubleType())]))
        # Esegui un'azione per misurare il tempo anche se non ci sono dati per il noop.save
        empty_final_df.write.format("noop").mode("overwrite").save()
        end_time_early = time.time()
        return empty_final_df, empty_hourly_df, end_time_early - start_time_func

    # Fase 2: Calcolo Percentili e Statistiche
    # Mappa a (country_code, (avg_ci, avg_cfe)) per raggruppare
    country_values_rdd = hourly_avg_rdd.map(
        lambda r: (r.country_code, (r.avg_carbon_intensity, r.avg_cfe))
    )

    # Raggruppa per country_code
    grouped_by_country_rdd = country_values_rdd.groupByKey()  # (country_code, ResultIterable[(avg_ci, avg_cfe)])

    def calculate_stats_for_country(item):
        country_code, iterable_values = item

        # Estrai liste di avg_ci e avg_cfe
        # Converti l'iterabile in lista per poterlo processare più volte
        values_list_of_tuples = list(iterable_values)

        ci_values = [tpl[0] for tpl in values_list_of_tuples if tpl[0] is not None]
        cfe_values = [tpl[1] for tpl in values_list_of_tuples if tpl[1] is not None]

        results = []

        # Statistiche per Carbon Intensity
        min_ci, p25_ci, p50_ci, p75_ci, max_ci = _calculate_percentiles_from_list(ci_values)
        if min_ci is not None:  # Solo se ci sono dati validi
            results.append(Row(
                country_code=country_code, data="carbon-intensity",
                min=min_ci, **{"25-perc": p25_ci, "50-perc": p50_ci, "75-perc": p75_ci}, max=max_ci
            ))

        # Statistiche per Carbon Free Percentage
        min_cfe, p25_cfe, p50_cfe, p75_cfe, max_cfe = _calculate_percentiles_from_list(cfe_values)
        if min_cfe is not None:  # Solo se ci sono dati validi
            results.append(Row(
                country_code=country_code, data="cfe",
                min=min_cfe, **{"25-perc": p25_cfe, "50-perc": p50_cfe, "75-perc": p75_cfe}, max=max_cfe
            ))
        return results

    # flatMap perché calculate_stats_for_country restituisce una lista di Row (0, 1 o 2 Row)
    final_stats_rdd = grouped_by_country_rdd.flatMap(calculate_stats_for_country)

    # Fase 3: Unione (implicita da flatMap) e Output Finale
    if final_stats_rdd.isEmpty():
        final_stats_df_q3 = spark_session.createDataFrame([], schema=FINAL_Q3_SCHEMA)
    else:
        final_stats_df_q3 = spark_session.createDataFrame(final_stats_rdd, schema=FINAL_Q3_SCHEMA)

    final_stats_df_q3.write.format("noop").mode("overwrite").save()

    # Creazione DataFrame dagli RDD orari medi per il secondo valore di ritorno
    # Questo potrebbe ricalcolare hourly_avg_rdd se non è più completamente in cache.
    # Per un benchmark più preciso di questa parte, si creerebbe prima dell'unpersist.
    hourly_avg_df_from_rdd = spark_session.createDataFrame(hourly_avg_rdd, schema=StructType([
        StructField("country_code", StringType()), StructField("hour", IntegerType()),
        StructField("avg_carbon_intensity", DoubleType()), StructField("avg_cfe", DoubleType())
    ]))
    hourly_avg_rdd.unpersist()

    end_time_func = time.time()
    exec_time = end_time_func - start_time_func

    return final_stats_df_q3, exec_time


def query3_rdd(num_executor):
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

    # Path specifico per i dati dell'Italia
    path_to_read = os.path.join(base_data_path, "country=Italy")

    execution_times_rdd = []
    output_df_q3_rdd = None

    print(f"\nEsecuzione della Query Q3 con RDD per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q3 con RDD - Run {i + 1}/{N_RUN}")
        output_df_q3_rdd, exec_time_rdd = run_query3_rdd(spark, path_to_read)
        execution_times_rdd.append(exec_time_rdd)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time_rdd:.4f} secondi.")

    avg_time_rdd = performance.print_performance(execution_times_rdd, N_RUN, "Q3 Spark RDD")
    performance.log_performance_to_csv(spark, "Q3", "rdd", avg_time_rdd, num_executor)

    if output_df_q3_rdd:
        print("\nRisultati finali per Q3 con RDD:")
        output_df_q3_rdd.show(n=output_df_q3_rdd.count(), truncate=False)

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()





