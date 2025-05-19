# ./spark_apps/preprocess_data.py
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, hour, to_timestamp, lit, when, upper

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
            when(zone_id_col.rlike("^[A-Z]{2}-"), upper(zone_id_col.substr(1, 2)))
            .otherwise(lit("UNKNOWN"))
        )
    return country_code

if __name__ == "__main__":
    start_time = time.time()
    spark = SparkSession.builder \
        .appName("DataPreprocessing") \
        .getOrCreate()

    base_input_path_hdfs = "hdfs://namenode:8020/project_data/electricity_maps/"
    print(f"Lettura dati da HDFS: {base_input_path_hdfs}*/*/*.parquet")

    try:
        df_raw = spark.read.parquet(base_input_path_hdfs + "*/*/*.parquet")
        if df_raw.rdd.isEmpty():
            print(f"ERRORE: Nessun file Parquet trovato in {base_input_path_hdfs}*/*/*.parquet")
            spark.stop()
            exit()
        print("Schema dati grezzi:")
        df_raw.printSchema()
        df_raw.show(5, truncate=False)
    except Exception as e:
        print(f"Errore lettura dati grezzi: {e}")
        spark.stop()
        exit()

    df_transformed = df_raw \
        .withColumn("datetime", to_timestamp(col("Datetime__UTC_"))) \
        .withColumn("carbon_intensity_direct", col("Carbon_intensity_gCO_eq_kWh__direct_").cast("double")) \
        .withColumn("carbon_free_percentage", col("Carbon_free_energy_percentage__CFE__").cast("double")) \
        .withColumn("year", year(col("datetime"))) \
        .withColumn("month", month(col("datetime"))) \
        .withColumn("hour", hour(col("datetime"))) \
        .withColumn("country_code", normalize_country_code(col("Country"), col("Zone_id"))) \
        .withColumnRenamed("Zone_id", "zone_id") \
        .select(
            "datetime", "zone_id", "carbon_intensity_direct", "carbon_free_percentage",
            "country_code", "year", "month", "hour"
        ) \
        .filter(col("country_code") != "UNKNOWN") # Rimuovi dati non mappabili

    print("Schema dati trasformati:")
    df_transformed.printSchema()
    df_transformed.show(5, truncate=False)
    print(f"Numero totale di righe trasformate: {df_transformed.count()}")
    print("Conteggio per country_code:")
    df_transformed.groupBy("country_code").count().orderBy("country_code").show(50)

    output_path_processed = "hdfs://namenode:8020/user/spark/processed_data/"
    try:
        df_transformed.write \
            .partitionBy("country_code") \
            .mode("overwrite") \
            .parquet(output_path_processed)
        print(f"Dati processati e partizionati salvati in {output_path_processed}")
    except Exception as e:
        print(f"Errore salvataggio dati processati: {e}")

    end_time = time.time()
    print(f"Tempo di esecuzione pre-processing: {end_time - start_time:.2f} secondi")
    spark.stop()