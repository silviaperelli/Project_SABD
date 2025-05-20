from pyspark.sql import SparkSession

# 1. Crea la SparkSession
spark = SparkSession.builder \
    .appName("Leggi e interroga HDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Leggi il file Parquet da HDFS (modifica il path se necessario)
df = spark.read.parquet("hdfs://namenode:8020/spark_data/spark/country=IT/part-00000-5177bd4a-295f-46d2-a464-84a7c1d3fa42.c000.snappy.parquet")

# 3. Registra il DataFrame come vista temporanea SQL
df.createOrReplaceTempView("dati")

# 4. Esegui una query SQL
risultato = spark.sql("SELECT * FROM dati LIMIT 10")

# 5. Mostra il risultato
risultato.show()
