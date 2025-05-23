import statistics
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator # Per Silhouette Score
import os

N_RUN_CLUSTERING = 5

# Lista dei 30 paesi
SELECTED_COUNTRIES = [
    # 15 Europei
    "Austria", "Belgium", "France", "Finland", "Germany", "Great Britain", "Ireland", "Italy", "Norway", "Poland",
    "Czechia", "Slovenia", "Spain", "Sweden", "Switzerland",
    # 15 Extra-Europei
    "USA", "Argentina", "Canada", "Mainland India", "South Korea", "Brazil", "Australia", "South Africa", "China", "Mexico",
    "Morocco", "Thailand", "United Arab Emirates", "Senegal", "Singapore"
]

def read_df(spark_session, paths_to_read, target_year=2024):
    print(f"Lettura dati per clustering da {len(paths_to_read)} partizioni.")

    try:
        # Leggiamo tutti i dati disponibili per i paesi
        df_all_countries = spark_session.read.parquet(*paths_to_read)

        if df_all_countries.rdd.isEmpty():
            print("ERRORE: Nessun dato trovato per i paesi selezionati.")
            # Schema di output: country_code, avg_carbon_intensity_2024, cluster_prediction
            schema_output = "country_code STRING, avg_carbon_intensity_2024 DOUBLE, cluster_prediction INTEGER"
            return spark_session.createDataFrame([], schema_output), None, 0.0
    except Exception as e:
        print(f"Errore durante la lettura dei dati per clustering: {e}")
        raise

    # 1. Filtra per l'anno target e aggrega per calcolare la media annua di carbon_intensity
    df_annual_avg = df_all_countries.where(F.col("year") == target_year) \
        .groupBy("country_code") \
        .agg(F.avg("carbon_intensity").alias("avg_carbon_intensity")) \
        .filter(F.col("avg_carbon_intensity").isNotNull())  # Rimuovi paesi senza dati per il 2024

    if df_annual_avg.rdd.isEmpty():
        print(f"ERRORE: Nessun dato di carbon intensity aggregato trovato per l'anno {target_year}.")
        schema_output = "country_code STRING, avg_carbon_intensity_2024 DOUBLE, cluster_prediction INTEGER"
        return spark_session.createDataFrame([], schema_output), None, 0.0

    # 2. Preparazione dati per K-Means
    assembler = VectorAssembler(
        inputCols=["avg_carbon_intensity"],
        outputCol="features",
        handleInvalid="skip"  # Salta righe con valori nulli nella feature
    )
    df_features = assembler.transform(df_annual_avg)

    if df_features.count() < 2:  # K-Means necessita di almeno k punti (e k >= 2 per Silhouette)
        print("ERRORE: Non abbastanza dati validi dopo la preparazione delle feature per il clustering.")
        schema_output = "country_code STRING, avg_carbon_intensity_2024 DOUBLE, cluster_prediction INTEGER"
        return spark_session.createDataFrame([], schema_output), None, 0.0

    return df_features

def tuning_k(spark_session, paths_to_read):
    start_time_tuning = time.time()

    df_features = read_df(spark_session, paths_to_read)

    df_features.cache()  # Cache perché lo useremo per trovare K e per il modello finale

    # 3. Determinazione del K Ottimale (usando Silhouette Score)
    print("\nDeterminazione del K ottimale usando Silhouette Score...")
    silhouette_scores = []
    schema_silhouette = "k INTEGER, silhouette_score DOUBLE"
    # Il numero massimo di cluster non può superare il numero di campioni (paesi)
    max_k_to_test = min(15, df_features.count())  # Testiamo fino a 15 cluster o num_paesi
    k_values = range(2, max_k_to_test + 1)

    for k_test in k_values:
        try:
            kmeans_test = KMeans().setK(k_test).setSeed(1).setFeaturesCol("features").setPredictionCol(
                "prediction_test")
            model_test = kmeans_test.fit(df_features)
            predictions_test = model_test.transform(df_features)

            evaluator = ClusteringEvaluator().setPredictionCol("prediction_test").setFeaturesCol(
                "features").setMetricName("silhouette").setDistanceMeasure("squaredEuclidean")
            silhouette = evaluator.evaluate(predictions_test)
            silhouette_scores.append({"k": k_test, "silhouette_score": silhouette})
            print(f"K={k_test}")
        except Exception as e_k:
            print(f"  Errore durante il test per K={k_test}: {e_k}")
            silhouette_scores.append({"k": k_test, "silhouette_score": -1})  # Valore indicativo di errore

    if not silhouette_scores:
        print("ERRORE: Nessun punteggio Silhouette calcolato. Impostazione K ottimale a 2 (default).")
        optimal_k = 2
        silhouette_results_df = spark_session.createDataFrame([], schema_silhouette)
    else:
        # Scegli il K con il Silhouette Score più alto
        silhouette_results_df = spark_session.createDataFrame(silhouette_scores, schema_silhouette)
        # Filtra i punteggi validi
        valid_silhouette_df = silhouette_results_df.where(F.col("silhouette_score") >= -1.0)
        if not valid_silhouette_df.rdd.isEmpty():
            best_k_row = valid_silhouette_df.orderBy(F.col("silhouette_score").desc()).first()
            if best_k_row:
                optimal_k = best_k_row["k"]
            else:  # Non dovrebbe accadere se valid_silhouette_results_df non è vuoto
                print("ERRORE: Impossibile determinare K ottimale. Impostazione K ottimale a 2 (default).")
                optimal_k = 2
        else:
            print("ERRORE: Tutti i tentativi di calcolo Silhouette sono falliti. Impostazione K ottimale a 2 (default).")
            optimal_k = 2  # Fallback se tutti i K falliscono

        print(f"K ottimale scelto: {optimal_k} (basato su Silhouette Score)")

    df_features.unpersist()  # Rilascia la cache

    end_time_tuning = time.time()

    return optimal_k, silhouette_results_df, end_time_tuning - start_time_tuning

def run_query_clustering(spark_session, paths_to_read, k):
    start_time = time.time()

    # Lettura dati da HDFS
    df_features = read_df(spark_session, paths_to_read)

    # 4. Addestramento del Modello K-Means Finale con K ottimale
    print(f"\nAddestramento del modello K-Means finale con K={k}...")
    kmeans_final = KMeans().setK(int(k)).setSeed(1).setFeaturesCol("features").setPredictionCol("cluster_prediction") # Assicura che K sia int
    model_final = kmeans_final.fit(df_features)

    # 5. Assegnazione Cluster
    predictions_final_df = model_final.transform(df_features)

    # 6. Preparazione Output
    output_df = predictions_final_df.select(
        F.col("country_code"),
        F.col("avg_carbon_intensity").alias(f"avg_carbon_intensity"),
        F.col("cluster_prediction")
    ).orderBy("cluster_prediction", f"avg_carbon_intensity")

    output_df.write.format("noop").mode("overwrite").save()

    end_time = time.time()

    return output_df, end_time - start_time


if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query_Clustering") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read = [os.path.join(base_data_path, f"country={country_code}") for country_code in SELECTED_COUNTRIES]

    execution_times_clustering = []
    final_output_clustering_df = None

    print(f"\nEsecuzione Tuning per Clustering...")
    optimal_k, silhouette_df, exec_time_tuning = tuning_k(spark, paths_to_read)

    print(f"\nEsecuzione della Query Clustering per {N_RUN_CLUSTERING} volte...")
    for i in range(N_RUN_CLUSTERING):
        print(f"Esecuzione Clustering - Run {i + 1}/{N_RUN_CLUSTERING}")
        try:
            result_clustering_df, exec_time = run_query_clustering(spark, paths_to_read, optimal_k)
            execution_times_clustering.append(exec_time)
            print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
            if i == N_RUN_CLUSTERING - 1: # Salva i risultati dell'ultimo run
                final_output_clustering_df = result_clustering_df
        except Exception as e:
            print(f"ERRORE durante l'esecuzione del Run {i + 1} per Clustering: {e}")
            break

    # Statistiche dei tempi
    if execution_times_clustering:
        avg_time_clustering = statistics.mean(execution_times_clustering)
        print(f"\n--- Statistiche Tempi Esecuzione Query Clustering  ---")
        print(f"Tempo di tuning: {exec_time_tuning:.4f} secondi")
        print(f"Tempi individuali: {[round(t, 4) for t in execution_times_clustering]}")
        print(f"Tempo medio di esecuzione in ({len(execution_times_clustering)} runs): {avg_time_clustering:.4f} secondi")
        if len(execution_times_clustering) > 1:
            std_dev_time_clustering = statistics.stdev(execution_times_clustering)
            print(f"Deviazione standard dei tempi: {std_dev_time_clustering:.4f} secondi")
        print("----------------------------------------------------")

    # Risultati Silhouette
    if silhouette_df is not None and not silhouette_df.rdd.isEmpty():
        print("\nRisultati Silhouette Score per K testati:")
        silhouette_df.show(truncate=False)

    # Output dei risultati del clustering
    if final_output_clustering_df:
        print("\nRisultati finali del Clustering (paese, carbon_intensity_2024, cluster):")
        try:
            num_rows_clustering = final_output_clustering_df.count()
            if num_rows_clustering > 0:
                final_output_clustering_df.show(n=num_rows_clustering, truncate=False)
                csv_output_path_clustering = os.path.join(base_data_path, "Q_Clustering_results.csv")
                final_output_clustering_df.coalesce(1).write.csv(csv_output_path_clustering, header=True, mode="overwrite")
                print(f"Risultati Clustering Q salvati in CSV: {csv_output_path_clustering}")
            else:
                print("DataFrame del clustering è vuoto.")
        except Exception as e:
            print(f"Errore durante la visualizzazione o il salvataggio dei risultati del clustering: {e}")

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()