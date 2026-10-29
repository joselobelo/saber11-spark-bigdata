# -*- coding: utf-8 -*-
"""
Consumidor Spark Structured Streaming - Anexo 3 de la guía (Tarea 3, Big Data UNAD).
Lee el topic 'sensor_data' de Kafka, convierte el JSON en columnas y calcula
la temperatura y la humedad promedio por sensor en ventanas de 1 minuto.

Ejecución (en otra terminal, con el productor ya corriendo):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

# Crear la sesión de Spark y reducir los mensajes INFO de la consola
spark = SparkSession.builder \
    .appName("SensorDataAnalysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos de entrada (el timestamp llega en segundos y Spark lo convierte)
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType())
])

# Lector de streaming desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Parsear los datos JSON del campo value
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Estadísticas por ventana de tiempo de 1 minuto y por sensor
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "sensor_id") \
    .agg({"temperature": "avg", "humidity": "avg"})

# Escribir los resultados en la consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
