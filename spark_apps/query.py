from pyspark.sql import SparkSession

# 1. Crea la SparkSession
spark = SparkSession.builder \
    .appName("Leggi e interroga HDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Leggi il file Parquet da HDFS (modifica il path se necessario)
df = spark.read.parquet("hdfs://namenode:8020/spark_data/spark/country=IT/part-00000-c7d351b0-f2e7-45a8-8ecc-f0cb1a5441ad.c000.snappy.parquet")

# 3. Registra il DataFrame come vista temporanea SQL
df.createOrReplaceTempView("dati")

# 4. Esegui una query SQL
risultato = spark.sql("SELECT * FROM dati LIMIT 10")

# 5. Mostra il risultato
risultato.show()
