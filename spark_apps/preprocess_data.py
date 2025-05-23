# ./spark_apps/preprocess_data.py
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, to_timestamp, lit, when, upper, avg

if __name__ == "__main__":
    start_time = time.time()
    spark = SparkSession.builder \
        .appName("DataPreprocessing") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_input_path_hdfs = "hdfs://namenode:8020/nifi_data/electricity_maps/"
    print(f"\nLettura dati da HDFS: {base_input_path_hdfs}*/*/*.parquet")

    try:
        df_raw = spark.read.parquet(base_input_path_hdfs + "*/*/*.parquet")
        if df_raw.rdd.isEmpty():
            print(f"ERRORE: Nessun file Parquet trovato in {base_input_path_hdfs}*/*/*.parquet")
            spark.stop()
            exit()

        columns_to_drop = [
            "Carbon_intensity_gCO_eq_kWh__Life_cycle_",
            "Renewable_energy_percentage__RE__",
            "Zone_name",
            "Data_source",
            "Data_estimated",
            "Data_estimation_method"
        ]

        existing_columns = df_raw.columns
        columns_to_drop_actual = [c for c in columns_to_drop if c in existing_columns]
        if len(columns_to_drop_actual) > 0:
            df_raw = df_raw.drop(*columns_to_drop_actual)
            print(f"\nColonne rimosse: {columns_to_drop_actual}")
        else:
            print("\nNessuna delle colonne specificate per la rimozione è presente nel DataFrame.")

    except Exception as e:
        print(f"\nErrore durante la lettura dei dati grezzi o la rimozione delle colonne: {e}")
        spark.stop()
        exit()

    df_intermediate = df_raw \
        .withColumn("datetime", to_timestamp(col("Datetime__UTC_"))) \
        .withColumn("carbon_intensity", col("Carbon_intensity_gCO_eq_kWh__direct_").cast("double")) \
        .withColumn("carbon_free_percentage", col("Carbon_free_energy_percentage__CFE__").cast("double")) \
        .withColumn("country_code", col("Zone_id")) \
        .withColumn("country", col("Country")) \

    df_final = df_intermediate \
        .withColumn("year", year(col("datetime"))) \
        .withColumn("month", month(col("datetime"))) \
        .withColumn("day", dayofmonth(col("datetime"))) \
        .withColumn("hour", hour(col("datetime"))) \
        .select(
            "datetime",
            "country_code",
            "country",
            "carbon_intensity",
            "carbon_free_percentage",
            "year",
            "month",
            "day",
            "hour"
        ).orderBy("country", "datetime") # Ordinamento per una migliore leggibilità dell'output

    print("\nSchema dati finali (aggregati per paese e timestamp):")
    df_final.printSchema()
    df_final.show(10, truncate=False)
    print(f"\nNumero totale di righe dopo aggregazione per paese e timestamp: {df_final.count()}")
    print("\nConteggio per country (dopo aggregazione per timestamp):")
    df_final.groupBy("country").count().orderBy("country").show(50)

    output_path_processed = "hdfs://namenode:8020/spark_data/spark"
    try:
        df_final.write \
            .partitionBy("country") \
            .mode("overwrite") \
            .parquet(output_path_processed)
        print(f"Dati aggregati per paese e timestamp, partizionati per paese, salvati in {output_path_processed}")
    except Exception as e:
        print(f"Errore salvataggio dati processati: {e}")

    end_time = time.time()
    print(f"\nTempo di esecuzione pre-processing e aggregazione per timestamp: {end_time - start_time:.2f} secondi")
    spark.stop()