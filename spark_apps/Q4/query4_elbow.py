import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
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
    print(f"Lettura dati per clustering da {len(paths_to_read)} partizioni")

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

    # Filtraggio per l'anno target e aggrega per calcolare la media annua di carbon_intensity
    df_annual_avg = df_all_countries.where(F.col("year") == target_year) \
        .groupBy("country_code") \
        .agg(F.avg("carbon_intensity").alias("avg_carbon_intensity")) \
        .filter(F.col("avg_carbon_intensity").isNotNull())  # Rimuovi paesi senza dati per il 2024

    if df_annual_avg.rdd.isEmpty():
        print(f"ERRORE: Nessun dato di carbon intensity aggregato trovato per l'anno {target_year}.")
        schema_output = "country_code STRING, avg_carbon_intensity_2024 DOUBLE, cluster_prediction INTEGER"
        return spark_session.createDataFrame([], schema_output), None, 0.0

    # Preparazione dati per K-Means
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


def run_query_clustering(spark_session, paths_to_read, k):
    start_time = time.time()

    # Lettura dati da HDFS
    df_features = read_df(spark_session, paths_to_read)

    # Addestramento del Modello K-Means Finale con K ottimale
    print(f"Addestramento del modello K-Means finale con K={k}")
    kmeans_final = KMeans().setK(int(k)).setSeed(1).setFeaturesCol("features").setPredictionCol("cluster_prediction") # Assicura che K sia int
    model_final = kmeans_final.fit(df_features)

    # Assegnazione Cluster
    predictions_final_df = model_final.transform(df_features)

    # Preparazione Output
    output_df = predictions_final_df.select(
        F.col("country_code"),
        F.col("avg_carbon_intensity").alias(f"avg_carbon_intensity"),
        F.col("cluster_prediction")
    ).orderBy("cluster_prediction", f"avg_carbon_intensity")

    output_df.write.format("noop").mode("overwrite").save()

    end_time = time.time()

    return output_df, end_time - start_time


def elbow_method(spark_session, paths_to_read):
    start_time_tuning = time.time()

    df_features = read_df(spark_session, paths_to_read)

    num_samples = df_features.count()

    if num_samples < 2:
        print(f"ERRORE: Numero di campioni ({num_samples}) insufficiente per il tuning K. Ritorno K=2 di default.")
        schema_wcss = "k INTEGER, wcss DOUBLE"
        return 2, spark_session.createDataFrame([], schema_wcss), time.time() - start_time_tuning

    df_features.cache()

    print("\nDeterminazione del K ottimale usando il metodo del Gomito (WCSS)...")
    wcss_scores = []
    schema_wcss = "k INTEGER, wcss DOUBLE"

    # Il numero massimo di cluster non può superare il numero di campioni
    # Testiamo da K=2 fino a min(15, num_samples)
    max_k_to_test = min(15, num_samples)
    k_values_range = range(2, max_k_to_test + 1)

    for k_test in k_values_range:
        try:
            kmeans_test = KMeans().setK(k_test).setSeed(1).setFeaturesCol("features").setPredictionCol("prediction_test")
            model_test = kmeans_test.fit(df_features)
            # WCSS (Within Cluster Sum of Squares) è accessibile come trainingCost
            wcss = model_test.summary.trainingCost
            wcss_scores.append({"k": k_test, "wcss": float(wcss)})
            print(f"  K={k_test}, WCSS={wcss:.4f}")
        except Exception as e:
            print(f"  Errore durante il test per K={k_test}: {e}")
            wcss_scores.append({"k": k_test, "wcss": float('inf')})

    df_features.unpersist()
    tuning_duration = time.time() - start_time_tuning

    if not wcss_scores:
        print("ERRORE: Nessun punteggio WCSS calcolato. Impostazione K ottimale a 2 (default).")
        return 2, spark_session.createDataFrame([], schema_wcss), tuning_duration

    wcss_results_df = spark_session.createDataFrame(wcss_scores, schema_wcss).orderBy("k")

    collected_wcss = wcss_results_df.filter(F.col("wcss") != float('inf')).collect()

    optimal_k = 2

    if len(collected_wcss) < 2:
        print("ERRORE: Non abbastanza punti WCSS validi per la selezione del gomito.")
        if collected_wcss:
            optimal_k = collected_wcss[0]["k"]
            print(f"  Utilizzo il primo K valido testato: {optimal_k}")
        else:
            print(f"  Impostazione K ottimale a {optimal_k} (default).")
    elif len(collected_wcss) == 2:  # Se ci sono solo due punti, scegliamo il K maggiore tra i due
        optimal_k = collected_wcss[1]["k"]
        print(f"Solo due valori di K testati validamente. Scelgo il K più alto: {optimal_k}")
    else:
        # Metodo della distanza dal segmento che unisce il primo e l'ultimo punto
        k_coords = [float(row["k"]) for row in collected_wcss]
        wcss_coords = [float(row["wcss"]) for row in collected_wcss]

        p1 = (k_coords[0], wcss_coords[0])  # Primo punto (k_min, wcss_at_k_min)
        pn = (k_coords[-1], wcss_coords[-1])  # Ultimo punto (k_max, wcss_at_k_max)

        distances = []
        for i in range(len(k_coords)):
            pk = (k_coords[i], wcss_coords[i])
            # Distanza del punto pk dalla linea definita da p1 e pn
            # Formula: |(y_n - y_1)*x_k - (x_n - x_1)*y_k + x_n*y_1 - y_n*x_1| / sqrt((y_n - y_1)^2 + (x_n - x_1)^2)
            numerator = abs((pn[1] - p1[1]) * pk[0] - (pn[0] - p1[0]) * pk[1] + pn[0] * p1[1] - pn[1] * p1[0])
            denominator = ((pn[1] - p1[1]) ** 2 + (pn[0] - p1[0]) ** 2) ** 0.5

            if denominator == 0:  # p1 e pn coincidono
                distances.append(0.0)
            else:
                distances.append(numerator / denominator)

        if distances:
            # L'indice del K ottimale è quello che massimizza la distanza
            optimal_k_index = distances.index(max(distances))
            optimal_k = int(k_coords[optimal_k_index])
        else:
            print("ERRORE: Calcolo delle distanze per il gomito fallito. Uso K di fallback.")
            optimal_k = int(k_coords[len(k_coords) // 2]) if k_coords else 2

    print(f"K ottimale scelto (metodo gomito): {optimal_k}")
    return optimal_k, wcss_results_df, tuning_duration


def query4_elbow(num_executor):
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("ProjectSABD_Query_Clustering") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", num_executor) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_data_path = "hdfs://namenode:8020/spark_data/spark"
    paths_to_read = [os.path.join(base_data_path, f"country={country_code}") for country_code in SELECTED_COUNTRIES]

    execution_times_clustering = []
    final_output_clustering_df = None

    print(f"\nEsecuzione Tuning per Clustering...")
    optimal_k, elbow_df, exec_time_tuning = elbow_method(spark, paths_to_read)

    print(f"\nEsecuzione della Query Clustering per {N_RUN} volte...")
    for i in range(N_RUN):
        print(f"\nEsecuzione Clustering - Run {i + 1}/{N_RUN}")
        try:
            result_clustering_df, exec_time = run_query_clustering(spark, paths_to_read, optimal_k)
            execution_times_clustering.append(exec_time)
            print(f"Run {i + 1} completato in {exec_time:.4f} secondi.")
            if i == N_RUN - 1: # Salva i risultati dell'ultimo run
                final_output_clustering_df = result_clustering_df
        except Exception as e:
            print(f"ERRORE durante l'esecuzione del Run {i + 1} per Clustering: {e}")
            break

    avg_time = performance.print_performance(execution_times_clustering, N_RUN, "Clustering")
    performance.log_performance_to_csv(spark, "Q4", "dataframe", avg_time, num_executor)

    # Risultati Silhouette
    if elbow_df is not None and not elbow_df.rdd.isEmpty():
        print("\nRisultati Elbow Method per K testati:")
        elbow_df.show(truncate=False)

    # Output dei risultati del clustering
    if final_output_clustering_df:
        print("\nRisultati finali del Clustering (paese, carbon_intensity_2024, cluster):")
        try:
            num_rows_clustering = final_output_clustering_df.count()
            if num_rows_clustering > 0:
                final_output_clustering_df.show(n=num_rows_clustering, truncate=False)
                csv_output_path_clustering = os.path.join(base_data_path, "Q4_elbow_results")
                final_output_clustering_df.coalesce(1).write.csv(csv_output_path_clustering, header=True, mode="overwrite")
                print(f"Risultati Clustering salvati in CSV: {csv_output_path_clustering}")
            else:
                print("DataFrame del clustering è vuoto.")
        except Exception as e:
            print(f"Errore durante la visualizzazione o il salvataggio dei risultati del clustering: {e}")

    end_time_script = time.time()
    print(f"\nTempo di esecuzione totale dello script: {end_time_script - start_time_script:.2f} secondi")

    spark.stop()