# -*- coding: utf-8 -*-
"""
Consumidor EXTENDIDO de Spark Structured Streaming (amplía el Anexo 3).

Sobre el topic 'sensor_data' calcula, por sensor y por ventana de 1 minuto:
  - número de eventos recibidos           (contar eventos)
  - temperatura promedio, mínima y máxima (estadísticas)
  - humedad promedio
  - alerta si la temperatura promedio supera un umbral (28 °C por defecto)
y además un resumen global por ventana (eventos totales y temperatura media).

Salidas:
  1. Consola (modo update): tabla por sensor y tabla global.
  2. salidas_streaming/ventanas.csv: estado completo de las ventanas, reescrito
     en cada micro-lote, para graficarlo con graficar_streaming.py.

Ejecución con Kafka (productor del Anexo 3 corriendo en otra terminal):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      spark_streaming_consumer_ext.py

Prueba sin Kafka (genera eventos con la fuente 'rate' de Spark):
  spark-submit spark_streaming_consumer_ext.py --sin-kafka --segundos 90
"""
import argparse
import csv
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

ESQUEMA = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType()),          # segundos de época Unix, como lo envía el productor
])


def fuente_kafka(spark, servidor, topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", servidor)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
            .select(F.col("value").cast("string").alias("json")))


def fuente_prueba(spark):
    """Simula el productor del Anexo 3 con la fuente 'rate' (5 eventos por segundo)."""
    r = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    return r.select(F.to_json(F.struct(
        ((F.col("value") % 10) + 1).cast("int").alias("sensor_id"),
        F.round(F.rand() * 10 + 20, 2).alias("temperature"),
        F.round(F.rand() * 40 + 30, 2).alias("humidity"),
        F.unix_timestamp(F.col("timestamp")).alias("timestamp"))).alias("json"))


def parsear(df_json):
    return (df_json.select(F.from_json("json", ESQUEMA).alias("d")).select("d.*")
            .withColumn("evento_ts", F.timestamp_seconds("timestamp"))
            .filter(F.col("sensor_id").isNotNull()))


def por_sensor(eventos, umbral):
    return (eventos
            .withWatermark("evento_ts", "2 minutes")
            .groupBy(F.window("evento_ts", "1 minute").alias("ventana"), "sensor_id")
            .agg(F.count("*").alias("eventos"),
                 F.round(F.avg("temperature"), 2).alias("temp_prom"),
                 F.round(F.min("temperature"), 2).alias("temp_min"),
                 F.round(F.max("temperature"), 2).alias("temp_max"),
                 F.round(F.avg("humidity"), 2).alias("hum_prom"))
            .withColumn("alerta", F.when(F.col("temp_prom") > umbral, "TEMP ALTA").otherwise("")))


def global_ventana(eventos):
    return (eventos
            .withWatermark("evento_ts", "2 minutes")
            .groupBy(F.window("evento_ts", "1 minute").alias("ventana"))
            .agg(F.count("*").alias("eventos_totales"),
                 F.approx_count_distinct("sensor_id").alias("sensores_activos"),
                 F.round(F.avg("temperature"), 2).alias("temp_prom_global")))


def escritor_csv(carpeta):
    os.makedirs(carpeta, exist_ok=True)
    ruta = os.path.join(carpeta, "ventanas.csv")

    def escribir(lote_df, lote_id):
        filas = (lote_df.select(F.date_format("ventana.start", "HH:mm").alias("inicio"), "sensor_id", "eventos",
                                "temp_prom", "temp_min", "temp_max", "hum_prom", "alerta")
                 .orderBy("inicio", "sensor_id").collect())
        tmp = ruta + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["inicio", "sensor_id", "eventos", "temp_prom", "temp_min", "temp_max", "hum_prom", "alerta"])
            for r in filas:
                w.writerow(list(r))
        os.replace(tmp, ruta)
        print("[micro-lote {}] {} filas -> {}".format(lote_id, len(filas), ruta))
    return escribir


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--servidor", default="localhost:9092")
    ap.add_argument("--topic", default="sensor_data")
    ap.add_argument("--umbral", type=float, default=28.0)
    ap.add_argument("--salida", default="salidas_streaming")
    ap.add_argument("--sin-kafka", action="store_true", help="prueba con la fuente 'rate' de Spark")
    ap.add_argument("--segundos", type=int, default=0, help="detener tras N segundos (0 = hasta Ctrl+C)")
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("SensorDataAnalysisExtendido")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    origen = fuente_prueba(spark) if a.sin_kafka else fuente_kafka(spark, a.servidor, a.topic)
    eventos = parsear(origen)

    q1 = (por_sensor(eventos, a.umbral)
          .select(F.date_format("ventana.start", "HH:mm:ss").alias("inicio"),
                  F.date_format("ventana.end", "HH:mm:ss").alias("fin"),
                  "sensor_id", "eventos", "temp_prom", "temp_min", "temp_max", "hum_prom", "alerta")
          .writeStream.outputMode("update").format("console")
          .option("truncate", False).option("numRows", 50)
          .trigger(processingTime="10 seconds").queryName("por_sensor").start())
    q2 = (global_ventana(eventos)
          .select(F.date_format("ventana.start", "HH:mm:ss").alias("inicio"),
                  "eventos_totales", "sensores_activos", "temp_prom_global")
          .writeStream.outputMode("update").format("console").option("truncate", False)
          .trigger(processingTime="10 seconds").queryName("global").start())
    q3 = (por_sensor(eventos, a.umbral)
          .writeStream.outputMode("complete").foreachBatch(escritor_csv(a.salida))
          .trigger(processingTime="10 seconds").queryName("csv").start())

    if a.segundos:
        spark.streams.awaitAnyTermination(a.segundos)
        for q in (q1, q2, q3):
            q.stop()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
