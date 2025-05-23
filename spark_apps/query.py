from pyspark.sql import SparkSession

# 1. Crea la SparkSession
spark = SparkSession.builder \
    .appName("Leggi e interroga HDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Leggi il file Parquet da HDFS (modifica il path se necessario)
df = spark.read.parquet("hdfs://namenode:8020/spark_data/spark/Q2_graphs/part-00000-2fb2b1ba-3682-4e55-9780-e91ac3c892d5-c000.csv")
#df = spark.read.parquet("hdfs://namenode:8020/nifi_data/electricity_maps/IT/2021/IT_2021_hourly.parquet")

# 3. Registra il DataFrame come vista temporanea SQL
df.createOrReplaceTempView("dati")

# 4. Esegui una query SQL
risultato = spark.sql("SELECT * FROM dati LIMIT 10")

# 5. Mostra il risultato
risultato.show()
