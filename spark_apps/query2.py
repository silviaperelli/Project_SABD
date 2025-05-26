import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

from performance import print_performance

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

def run_query2(spark_session, path_to_read):
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

    # Aggregazione per (anno, mese)
    monthly_aggregated_it_df = df_processed.groupBy("year", "month") \
        .agg(
        F.avg("carbon_intensity").alias("avg_carbon_intensity"),
        F.avg("carbon_free_percentage").alias("avg_cfe")) \
        .withColumn("date", F.concat(F.col("year"), F.lit("_"), F.lpad(F.col("month"), 2, '0'))) \
        .select("date", "avg_carbon_intensity", "avg_cfe", "year", "month")  # Mantenere year e month per ordinamenti

    monthly_aggregated_it_df.cache()

    # 1. Carbon intensity decrescente (peggiori)
    ci_desc = monthly_aggregated_it_df.orderBy(F.col("avg_carbon_intensity").desc()) \
        .limit(5)
    ci_desc_output = format_for_output(ci_desc)

    # 2. Carbon intensity crescente (migliori)
    ci_asc = monthly_aggregated_it_df.orderBy(F.col("avg_carbon_intensity").asc()) \
        .limit(5)
    ci_asc_output = format_for_output(ci_asc)

    # 3. Carbon-free energy percentage decrescente (migliori)
    cfe_desc = monthly_aggregated_it_df.orderBy(F.col("avg_cfe").desc()) \
        .limit(5)
    cfe_desc_output = format_for_output(cfe_desc)

    # 4. Carbon-free energy percentage crescente (peggiori)
    cfe_asc = monthly_aggregated_it_df.orderBy(F.col("avg_cfe").asc()) \
        .limit(5)
    cfe_asc_output = format_for_output(cfe_asc)

    # Unione di tutti i DataFrame per l'output unico
    final_df_q2 = ci_desc_output \
        .unionAll(ci_asc_output) \
        .unionAll(cfe_desc_output) \
        .unionAll(cfe_asc_output)

    monthly_aggregated_it_df.unpersist()  # Rimozione dalla cache

    final_df_q2.write.format("noop").mode("overwrite").save()

    end_time = time.time()

    return final_df_q2, monthly_aggregated_it_df, end_time - start_time

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


if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Path base ai dati processati e partizionati
    base_data_path = "hdfs://namenode:8020/spark_data/spark"

    # Path specifico per i dati dell'Italia
    path_to_read = os.path.join(base_data_path, "country=Italy")

    execution_times = []  # Lista per memorizzare i tempi di ogni esecuzione della query
    final_output_df_q2 = None  # Per salvare il risultato dell'ultima esecuzione
    final_monthly_df = None # Per salvare il dataframe aggregato su coppia (anno, mese)

    print(f"\nEsecuzione della Query Q2 per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q2 - Run {i + 1}/{N_RUN}")

        result_df, monthly_df, exec_time = run_query2(spark, path_to_read)
        execution_times.append(exec_time)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
        if i == N_RUN - 1:  # Se è l'ultima esecuzione, salva il DataFrame risultato
            final_output_df_q2 = result_df
            final_monthly_df = monthly_df

    print_performance(execution_times, N_RUN, "Q2")

    if final_output_df_q2 and final_monthly_df:
        print("\nRisultati aggregati finali per Q2:")

        final_output_df_q2.show(n=final_output_df_q2.count(), truncate=False)

        csv_output_path = os.path.join(base_data_path, "Q2_results")  # Path per il CSV
        csv_graphs_path = os.path.join(base_data_path, "Q2_graphs") # Path per il CSV per i grafici
        # .coalesce(1) riduce il numero di partizioni a 1 per scrivere un singolo file CSV
        final_output_df_q2.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")
        final_monthly_df.coalesce(1).write.csv(csv_graphs_path, header=True, mode="overwrite")
        print(f"Risultati Q2 salvati in CSV: {csv_output_path} e in {csv_graphs_path}")

    execution_times_sql = []
    output_df_q2_sql = None

    print(f"\nEsecuzione della Query Q2 con Spark SQL per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Q2 SQL - Run {i + 1}/{N_RUN}")
        output_df_q2_sql, exec_time_sql = run_query2_spark_sql(spark, path_to_read)
        execution_times_sql.append(exec_time_sql)  # Aggiunge il tempo di esecuzione alla lista
        print(f"Run {i + 1} completato in {exec_time_sql:.4f} secondi.")

    print_performance(execution_times_sql, N_RUN, "Q2 Spark SQL")

    if output_df_q2_sql:
        print("\nRisultati finali per Q2 con Spark SQL:")
        output_df_q2_sql.show(n=output_df_q2_sql.count(), truncate=False)

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()





