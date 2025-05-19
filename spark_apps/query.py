from pyspark.sql import SparkSession

# 1. Crea la SparkSession
spark = SparkSession.builder \
    .appName("Leggi e interroga HDFS") \
    .getOrCreate()

# 2. Leggi il file Parquet da HDFS (modifica il path se necessario)
df = spark.read.parquet("hdfs://namenode:8020/user/spark/processed_data/country_code=IT/part-00001-0a2d1db6-985d-4d7f-8912-fe64028ced3f.c000.snappy.parquet")

# 3. Registra il DataFrame come vista temporanea SQL
df.createOrReplaceTempView("dati")

# 4. Esegui una query SQL
risultato = spark.sql("SELECT * FROM dati LIMIT 10")

# 5. Mostra il risultato
risultato.show()
