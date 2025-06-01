import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import math

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
        # a, b sono (sum_ci, sum_cfe, count)
        return (a[0] + b[0], a[1] + b[1], a[2] + b[2])

    reduced_hourly_rdd = mapped_hourly_rdd.reduceByKey(reduce_hourly_aggregates)

    def calculate_hourly_averages(item):
        key, value = item
        hour, country_code = key
        sum_ci, sum_cfe, count = value

        avg_ci = sum_ci / count if count > 0 else None
        avg_cfe = sum_cfe / count if count > 0 else None

        return Row(country_code=country_code, hour=hour, avg_carbon_intensity=avg_ci, avg_cfe=avg_cfe)

    hourly_avg_rdd = reduced_hourly_rdd.map(calculate_hourly_averages)
    hourly_avg_rdd.cache()

    # Calcolo Percentili e Statistiche

    # Mappa a (country_code, (avg_ci, avg_cfe)) per raggruppare
    country_values_rdd = hourly_avg_rdd.map(
        lambda r: (r.country_code, (r.avg_carbon_intensity, r.avg_cfe))
    )

    # Raggruppa per country_code
    grouped_by_country_rdd = country_values_rdd.groupByKey()  # (country_code, ResultIterable[(avg_ci, avg_cfe)])

    def calculate_stats_for_country(item):
        country_code, iterable_values = item
        values_list_of_tuples = list(iterable_values)
        ci_values = [tpl[0] for tpl in values_list_of_tuples if tpl[0] is not None]
        cfe_values = [tpl[1] for tpl in values_list_of_tuples if tpl[1] is not None]
        results = []

        def get_interpolated_percentiles(values):
            # Assicura che siano float e ordina
            sorted_values = sorted([float(v) for v in values])
            n = len(sorted_values)

            min_val = sorted_values[0]
            max_val = sorted_values[-1]

            if n == 1:
                return min_val, min_val, min_val, min_val, max_val

            percentiles_to_calc = [0.25, 0.50, 0.75]
            calculated_p_values = []

            for p in percentiles_to_calc:
                index = p * (n - 1)

                lower_idx = math.floor(index)
                upper_idx = math.ceil(index)

                # Gestione limiti: se l'indice è esattamente l'ultimo elemento
                if lower_idx == n - 1:
                    calculated_p_values.append(sorted_values[lower_idx])
                    continue

                fractional_part = index - lower_idx

                if lower_idx == upper_idx:
                    calculated_p_values.append(sorted_values[int(lower_idx)])
                else:
                    # Interpolazione lineare
                    interpolated_value = (1 - fractional_part) * sorted_values[int(lower_idx)] + fractional_part * sorted_values[int(upper_idx)]
                    calculated_p_values.append(interpolated_value)

            p25_val, p50_val, p75_val = calculated_p_values
            return min_val, p25_val, p50_val, p75_val, max_val

        # Calcolo per Carbon Intensity
        min_ci, p25_ci, p50_ci, p75_ci, max_ci = get_interpolated_percentiles(ci_values)
        if min_ci is not None:
            results.append(Row(
                country_code=country_code, data="carbon-intensity",
                min=min_ci, **{"25-perc": p25_ci, "50-perc": p50_ci, "75-perc": p75_ci}, max=max_ci
            ))

        # Calcolo per Carbon Free Percentage
        min_cfe, p25_cfe, p50_cfe, p75_cfe, max_cfe = get_interpolated_percentiles(cfe_values)
        if min_cfe is not None:
            results.append(Row(
                country_code=country_code, data="cfe",
                min=min_cfe, **{"25-perc": p25_cfe, "50-perc": p50_cfe, "75-perc": p75_cfe}, max=max_cfe
            ))
        return results


    final_stats_rdd = grouped_by_country_rdd.flatMap(calculate_stats_for_country)

    if final_stats_rdd.isEmpty():
        final_stats_df_q3 = spark_session.createDataFrame([], schema=FINAL_Q3_SCHEMA)
    else:
        final_stats_df_q3 = spark_session.createDataFrame(final_stats_rdd, schema=FINAL_Q3_SCHEMA)

    final_stats_df_q3.write.format("noop").mode("overwrite").save()

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

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read = [
        os.path.join(base_data_path, "country=Italy"),
        os.path.join(base_data_path, "country=Sweden")
    ]

    execution_times_rdd = []
    output_df_q3_rdd = None

    print(f"\nEsecuzione della Query Q3 con RDD per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q3 con RDD - Run {i + 1}/{N_RUN}")
        output_df_q3_rdd, exec_time_rdd = run_query3_rdd(spark, paths_to_read)
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





