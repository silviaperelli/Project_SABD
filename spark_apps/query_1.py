import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Path base ai dati processati e partizionati
    base_processed_path = "hdfs://namenode:8020/spark_data/spark"

    paths_to_read = [
        os.path.join(base_processed_path, "country=IT"),
        os.path.join(base_processed_path, "country=SE")
    ]

    print(f"Lettura dati dalle partizioni specifiche in HDFS: {paths_to_read}")
    try:
        # Leggi i dati Parquet specificando una lista di path
        df_processed = spark.read.parquet(*paths_to_read)

        if df_processed.rdd.isEmpty():
            print(f"ERRORE: Nessun dato trovato nelle partizioni specificate: {paths_to_read}")
            spark.stop()
            exit()
        print("Schema dei dati letti dalle partizioni IT e SE:")
        df_processed.printSchema()

    except Exception as e:
        print(f"Errore durante la lettura dei dati dalle partizioni: {e}")
        spark.stop()
        exit()

    # Query Q1: Aggregare i dati su base annua
    # Calcolare media, min, max di "Carbon intensity" e "Carbon-free energy percentage"
    # per ciascun anno dal 2021 al 2024.

    print("\nEsecuzione della Query Q1...")

    # Esegui l'aggregazione
    annual_aggregated_df = df_processed.groupBy("year", "country_code") \
        .agg(
        F.avg("carbon_intensity").alias("carbon_mean"),
        F.min("carbon_intensity").alias("carbon_min"),
        F.max("carbon_intensity").alias("carbon_max"),
        F.avg("carbon_free_percentage").alias("cfe_mean"),
        F.min("carbon_free_percentage").alias("cfe_min"),
        F.max("carbon_free_percentage").alias("cfe_max")
    ) \
        .orderBy("country_code", "year")

    print("Risultati aggregati per Q1:")
    annual_aggregated_df_renamed = annual_aggregated_df.withColumnRenamed("year", "date")

    output_df_q1 = annual_aggregated_df_renamed.select(
        "date", "country_code",
        "carbon_mean", "carbon_min", "carbon_max",
        "cfe_mean", "cfe_min", "cfe_max"
    )

    output_df_q1.show(n=output_df_q1.count(), truncate=False)

    # Salvataggio CSV
    csv_output_path = os.path.join(base_processed_path, "Q1")
    if not os.path.exists(csv_output_path):
        os.makedirs(csv_output_path)

    # Salvataggio in un singolo file CSV
    output_df_q1.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")
    print(f"Risultati Q1 salvati in CSV: {csv_output_path}")

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()