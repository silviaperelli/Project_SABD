import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Path base ai dati processati e partizionati
    base_data_path = "hdfs://namenode:8020/spark_data/spark"

    # Path specifico per i dati dell'Italia
    paths_to_read = os.path.join(base_data_path, "country=IT")

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {paths_to_read}")
    try:
        df_processed = spark.read.parquet(paths_to_read)

        if df_processed.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nella partizione specificata: {paths_to_read}")
            spark.stop()
            exit()
        print("Schema dei dati letti dalla partizione IT:")
        df_processed.printSchema()

    except Exception as e:
        print(f"Errore durante la lettura dei dati dalla partizione IT: {e}")
        spark.stop()
        exit()

    # Query Q2: Aggregare i dati sulla coppia (anno, mese)
    # Calcolare media "Carbon intensity" e "Carbon-free energy percentage" per coppia (anno, mese)
    # Calcolare la classifica delle prime 5 coppie ordinando per “Carbon intensity” decrescente, crescente e
    # “Carbon-free energy percentage” decrescente, crescente.

    print("\nEsecuzione della Query Q2 per l'Italia (output unico file)...")

    # Aggregazione per (anno, mese)
    monthly_aggregated_it_df = df_processed.groupBy("year", "month") \
        .agg(
        F.avg("carbon_intensity").alias("avg_carbon_intensity"),
        F.avg("carbon_free_percentage").alias("avg_cfe")
    ) \
        .withColumn("date_str", F.concat(F.col("year"), F.lit("_"), F.lpad(F.col("month"), 2, '0'))
    ) \
        .select("date_str", "avg_carbon_intensity", "avg_cfe", "year", "month")  # Mantenere year e month per ordinamenti

    monthly_aggregated_it_df.cache()

    print("\nDati mensili aggregati per l'Italia (primi record):")
    monthly_aggregated_it_df.orderBy("year", "month").show(5, truncate=False)

    # Preparazione colonne finali come da esempio output ("date", "carbon-intensity", "cfe")
    def format_for_output(df_input):
        return df_input.select(
            F.col("date_str").alias("date"),
            F.col("avg_carbon_intensity").alias("carbon_intensity"),
            F.col("avg_cfe").alias("cfe")
        )

    # 1. Carbon intensity decrescente (peggiori)
    ci_desc = monthly_aggregated_it_df.orderBy(F.col("avg_carbon_intensity").desc()) \
        .limit(5)
    ci_desc_output = format_for_output(ci_desc)
    print("\nTop 5 (anno, mese) per Carbon Intensity (decrescente - peggiori):")
    ci_desc_output.show(truncate=False)

    # 2. Carbon intensity crescente (migliori)
    ci_asc = monthly_aggregated_it_df.orderBy(F.col("avg_carbon_intensity").asc()) \
        .limit(5)
    ci_asc_output = format_for_output(ci_asc)
    print("\nTop 5 (anno, mese) per Carbon Intensity (crescente - migliori):")
    ci_asc_output.show(truncate=False)

    # 3. Carbon-free energy percentage decrescente (migliori)
    cfe_desc = monthly_aggregated_it_df.orderBy(F.col("avg_cfe").desc()) \
        .limit(5)
    cfe_desc_output = format_for_output(cfe_desc)
    print("\nTop 5 (anno, mese) per Carbon-Free Energy % (decrescente - migliori):")
    cfe_desc_output.show(truncate=False)

    # 4. Carbon-free energy percentage crescente (peggiori)
    cfe_asc = monthly_aggregated_it_df.orderBy(F.col("avg_cfe").asc()) \
        .limit(5)
    cfe_asc_output = format_for_output(cfe_asc)
    print("\nTop 5 (anno, mese) per Carbon-Free Energy % (crescente - peggiori):")
    cfe_asc_output.show(truncate=False)

    # 5. Dati mensili aggregati completi, ordinati per data
    # all_monthly_data_ordered = monthly_aggregated_it_df.orderBy("year", "month")
    # all_monthly_data_output = format_for_output(all_monthly_data_ordered)
    # print("\nDati mensili aggregati completi per grafici (primi record):")
    # all_monthly_data_output.show(5, truncate=False)

    # Creazione DataFrame separatore con spazi vuoti
    output_schema = ci_desc_output.schema

    # Riempiamo i campi del CSV con stringa vuota e None per le colonne numeriche
    separator_data = [("", None, None)]  # (date, carbon_intensity, cfe)
    separator_df = spark.createDataFrame(separator_data, schema=output_schema)

    # Unione di tutti i DataFrame per l'output unico
    final_df_q2 = ci_desc_output \
        .unionAll(separator_df) \
        .unionAll(ci_asc_output) \
        .unionAll(separator_df) \
        .unionAll(cfe_desc_output) \
        .unionAll(separator_df) \
        .unionAll(cfe_asc_output)

    print("\nSchema del DataFrame finale combinato per Q2:")
    final_df_q2.printSchema()
    print(f"Numero totale di righe nel DataFrame finale: {final_df_q2.count()}")

    # Path di output per il file CSV della Q2
    csv_output_path = os.path.join(base_data_path, "Q2")
    if not os.path.exists(csv_output_path):
        os.makedirs(csv_output_path)

    # Salvataggio in un singolo file CSV
    final_df_q2.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")
    print(f"Risultati Q2 salvati in CSV: {csv_output_path}")

    monthly_aggregated_it_df.unpersist()  # Rimuovi dalla cache

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script Q2: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()