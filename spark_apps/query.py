from pyspark.sql import SparkSession

# 1. Crea la SparkSession
spark = SparkSession.builder \
    .appName("Leggi e interroga HDFS") \
    .getOrCreate()

# 2. Leggi il file Parquet da HDFS (modifica il path se necessario)
df = spark.read.parquet("hdfs://namenode:8020/spark_data/spark/country_code=IT/part-00000-ac6114c5-28e9-4c19-b961-1517331bc3cc.c000.snappy.parquet")

# 3. Registra il DataFrame come vista temporanea SQL
df.createOrReplaceTempView("dati")

# 4. Esegui una query SQL
risultato = spark.sql("SELECT * FROM dati LIMIT 10")

# 5. Mostra il risultato
risultato.show()
