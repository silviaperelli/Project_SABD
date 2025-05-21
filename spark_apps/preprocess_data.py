# ./spark_apps/preprocess_data.py
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, to_timestamp, lit, when, upper, avg

def normalize_country_code(country_col_original, zone_id_col):
    """
    Normalizzazione del nome del paese in un codice ISO a 2 lettere (maiuscolo).
    """
    country_code = when(upper(country_col_original) == "ITALY", lit("IT")) \
        .when(upper(country_col_original) == "SWEDEN", lit("SE")) \
        .when(upper(country_col_original) == "GERMANY", lit("DE")) \
        .when(upper(country_col_original) == "FRANCE", lit("FR")) \
        .when(upper(country_col_original) == "SPAIN", lit("ES")) \
        .when(upper(country_col_original) == "AUSTRIA", lit("AT")) \
        .when(upper(country_col_original) == "BELGIUM", lit("BE")) \
        .when(upper(country_col_original) == "FINLAND", lit("FI")) \
        .when(upper(country_col_original) == "UNITED KINGDOM", lit("GB")) \
        .when(upper(country_col_original) == "IRELAND", lit("IE")) \
        .when(upper(country_col_original) == "NORWAY", lit("NO")) \
        .when(upper(country_col_original) == "POLAND", lit("PL")) \
        .when(upper(country_col_original) == "CZECH REPUBLIC", lit("CZ")) \
        .when(upper(country_col_original) == "SLOVENIA", lit("SI")) \
        .when(upper(country_col_original) == "SWITZERLAND", lit("CH")) \
        .when(upper(country_col_original) == "UNITED STATES", lit("US")) \
        .when(upper(country_col_original) == "UNITED ARAB EMIRATES", lit("AE")) \
        .when(upper(country_col_original) == "CHINA", lit("CN")) \
        .when(upper(country_col_original) == "INDIA", lit("IN")) \
        .otherwise(
        when(zone_id_col.isNotNull() & zone_id_col.rlike("^[A-Z]{2}"), upper(zone_id_col.substr(1, 2))) # Gestione più robusta per zone_id
        .otherwise(lit("UNKNOWN"))
    )
    return country_code


if __name__ == "__main__":
    start_time = time.time()
    spark = SparkSession.builder \
        .appName("DataPreprocessing") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_input_path_hdfs = "hdfs://namenode:8020/nifi_data/electricity_maps/"
    print(f"Lettura dati da HDFS: {base_input_path_hdfs}*/*/*.parquet")

    try:
        df_raw = spark.read.parquet(base_input_path_hdfs + "*/*/*.parquet")
        if df_raw.rdd.isEmpty():
            print(f"ERRORE: Nessun file Parquet trovato in {base_input_path_hdfs}*/*/*.parquet")
            spark.stop()
            exit()

        print("Schema dati grezzi (prima della rimozione colonne):")
        df_raw.printSchema()

        columns_to_drop = [
            "Carbon_intensity_gCO_eq_kWh__Life_cycle_",
            "Renewable_energy_percentage__RE__",
            "Data_source",
            "Data_estimated",
            "Data_estimation_method"
        ]

        existing_columns = df_raw.columns
        columns_to_drop_actual = [c for c in columns_to_drop if c in existing_columns]
        if len(columns_to_drop_actual) > 0:
            df_raw = df_raw.drop(*columns_to_drop_actual)
            print(f"Colonne rimosse: {columns_to_drop_actual}")
        else:
            print("Nessuna delle colonne specificate per la rimozione è presente nel DataFrame.")


        print("Schema dati grezzi (dopo la rimozione colonne):")
        df_raw.printSchema()
        df_raw.show(5, truncate=False)

    except Exception as e:
        print(f"Errore durante la lettura dei dati grezzi o la rimozione delle colonne: {e}")
        spark.stop()
        exit()

    df_intermediate = df_raw \
        .withColumn("datetime", to_timestamp(col("Datetime__UTC_"))) \
        .withColumn("carbon_intensity_val", col("Carbon_intensity_gCO_eq_kWh__direct_").cast("double")) \
        .withColumn("carbon_free_percentage_val", col("Carbon_free_energy_percentage__CFE__").cast("double")) \
        .withColumn("country_code", normalize_country_code(col("Country"), col("Zone_id"))) \
        .filter(col("country_code") != "UNKNOWN") \
        .select(
            "datetime",
            "country_code",
            "carbon_intensity_val",
            "carbon_free_percentage_val"
        )

    print("Schema dati dopo trasformazioni iniziali e filtro (pronto per l'aggregazione per timestamp):")
    df_intermediate.printSchema()
    df_intermediate.show(5, truncate=False)

    df_aggregated_by_timestamp = df_intermediate \
        .groupBy("datetime", "country_code") \
        .agg(
            avg("carbon_intensity_val").alias("carbon_intensity"),
            avg("carbon_free_percentage_val").alias("carbon_free_percentage")
        )

    print("Schema dati dopo l'aggregazione per paese e timestamp:")
    df_aggregated_by_timestamp.printSchema()
    df_aggregated_by_timestamp.show(5, truncate=False)

    df_final = df_aggregated_by_timestamp \
        .withColumn("year", year(col("datetime"))) \
        .withColumn("month", month(col("datetime"))) \
        .withColumn("day", dayofmonth(col("datetime"))) \
        .withColumn("hour", hour(col("datetime"))) \
        .select(
            "datetime",
            "country_code",
            col("country_code").alias("country"),
            "carbon_intensity",
            "carbon_free_percentage",
            "year",
            "month",
            "day",
            "hour"
        ).orderBy("country", "datetime") # Ordinamento per una migliore leggibilità dell'output


    print("Schema dati finali (aggregati per paese e timestamp):")
    df_final.printSchema()
    df_final.show(10, truncate=False)
    print(f"Numero totale di righe dopo aggregazione per paese e timestamp: {df_final.count()}")
    print("Conteggio per country (dopo aggregazione per timestamp):")
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
    print(f"Tempo di esecuzione pre-processing e aggregazione per timestamp: {end_time - start_time:.2f} secondi")
    spark.stop()