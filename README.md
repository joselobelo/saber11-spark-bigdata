# Brechas en Saber 11 con Apache Spark · Tarea 3 Big Data (UNAD)

Procesamiento **en batch** con Apache Spark (PySpark) y **en tiempo real** con Apache Kafka y Spark Structured Streaming.
Curso Big Data (202016911), Universidad Nacional Abierta y a Distancia (UNAD).

Autor: `[Nombre del estudiante]` · Grupo: `[202016911_XX]`

---

## 1. Problema

En Colombia, el examen Saber 11 mide las competencias de quienes terminan la educación media, y su puntaje global (de 0 a 500) incide en el acceso a la educación superior y a becas. El análisis busca responder:

> **¿Qué tan grandes son las brechas en el puntaje global de Saber 11 según el estrato socioeconómico, la naturaleza (oficial o no oficial) y la zona (urbana o rural) del colegio, el departamento, el acceso a internet y computador y el nivel educativo de la madre?**

Con más de dos millones de registros, el volumen supera lo que una hoja de cálculo maneja con comodidad y justifica el uso de un motor distribuido como Spark.

## 2. Conjunto de datos

| Atributo | Valor |
|---|---|
| Nombre | Resultados únicos Saber 11 |
| Publicador | Instituto Colombiano para la Evaluación de la Educación (ICFES) |
| Fuente original | Datos Abiertos Colombia · https://www.datos.gov.co/d/kgxf-xxbe |
| Licencia | Creative Commons BY-SA 4.0 |
| Cobertura total | 2010-2022 · 7.109.704 filas · 51 columnas (todas de tipo texto) |
| Alcance de este análisis | Periodos 2019-2022 (`periodo >= '20191'`) · **2.225.985 filas** · 21 columnas descargadas, 17 usadas en el análisis |
| Tamaño aproximado de la descarga | 450-550 MB en CSV |

**Problemas de calidad encontrados en la fuente** (verificados con consultas a la API SODA el 24 de octubre de 2026) y cómo los resuelve el código:

| Problema | Evidencia | Solución en `saber11_batch.py` |
|---|---|---|
| Registros duplicados | El periodo 20194 tiene 1.096.524 filas pero 546.211 estudiantes únicos; el 20224, 1.065.888 filas y 532.792 únicos | `dropDuplicates(["estu_consecutivo"])` |
| Mismo departamento escrito de dos formas | `BOGOTA` (2.446 filas) y `BOGOTÁ` (330.259 filas) | Mayúsculas, sin tildes y sin espacios (`F.translate`) |
| Puntajes como texto | Las 51 columnas son de tipo texto | `cast(IntegerType())` y filtro 0-500 |
| Categorías no informativas | `Sin Estrato`, `No sabe`, `No Aplica` y valores vacíos | Se recodifican o se excluyen de cada análisis |
| Puntaje global ausente antes de 2014-2 | `punt_global` no existe en los periodos 2010-20141 | El análisis usa 2019-2022 |

## 3. Arquitectura de la solución

```
datos.gov.co (API SODA)          Máquina virtual (Hadoop + Spark de la Tarea 2)
┌──────────────────┐   descarga  ┌──────────────┐    ┌───────────────────────────┐    ┌──────────────────┐
│ kgxf-xxbe (CSV)  │ ──────────▶ │ HDFS /Tarea3 │ ─▶ │ saber11_batch.py (PySpark)│ ─▶ │ salidas/         │
└──────────────────┘             └──────────────┘    │ limpieza · EDA · RDD      │    │ gráficos, CSV,   │
                                                     └───────────────────────────┘    │ reporte.html     │
kafka_producer.py ─▶ Kafka topic sensor_data ─▶ spark_streaming_consumer(_ext).py ─▶ consola + gráfico  │
```

## 4. Estructura del repositorio

```
saber11-spark-bigdata/
├── README.md
├── requirements.txt
├── datos/
│   └── descargar_saber11.py          # descarga desde la API oficial, paginada (solo biblioteca estándar)
├── scripts/
│   └── ejecutar_batch.sh             # descarga → HDFS → spark-submit en un solo paso
├── batch/
│   └── saber11_batch.py              # aplicación principal de procesamiento en batch
├── streaming/
│   ├── kafka_producer.py             # productor del Anexo 3 (topic sensor_data)
│   ├── spark_streaming_consumer.py   # consumidor del Anexo 3 (promedios por ventana de 1 min)
│   ├── spark_streaming_consumer_ext.py  # versión extendida: conteos, mín/máx, alertas, CSV
│   └── graficar_streaming.py         # gráfico de los resultados en tiempo real
└── pruebas/
    └── generar_datos_prueba.py       # CSV SINTÉTICO solo para probar el código (no son datos reales)
```

## 5. Qué hace el procesamiento en batch

| Etapa | Operaciones Spark |
|---|---|
| 1. Carga | `spark.read.csv` desde HDFS, esquema de texto, número de particiones de lectura |
| 2. Perfil de calidad | `count`, `countDistinct`, nulos por columna, duplicados por periodo |
| 3. Limpieza y transformación | `dropDuplicates`, `cast`, `filter`, normalización de texto, columnas derivadas (`anio`, `estrato`, `internet`, `computador`, `educacion_madre`, `nivel_desempeno`), `persist` en memoria y disco |
| 4. Análisis exploratorio (DataFrames) | `describe`, `approxQuantile`, `groupBy().agg()` por año, calendario, estrato, naturaleza y zona, departamento, internet y computador, educación de la madre y género; correlación de Pearson entre áreas (`stat.corr`); `pivot` de niveles de desempeño |
| 5. Operaciones con RDD | `map`, `filter`, `flatMap`, `groupByKey`, `reduceByKey`, `sortBy`, `collect`, `count`, `take`, `reduce` |
| 6. Visualización | 8 gráficos PNG, 15 tablas CSV y `salidas/reporte.html` |

## 6. Requisitos

- Máquina virtual de la Tarea 2 con Hadoop (HDFS en `hdfs://localhost:9000`) y Apache Spark 3.5.x.
- Python 3.8 o superior y `pip`.
- Conexión a internet para descargar los datos.

```bash
pip3 install -r requirements.txt       # matplotlib (gráficos) y kafka-python (productor del Anexo 3)
```

## 7. Ejecución del procesamiento en batch

```bash
# 0) Clonar el repositorio en la máquina virtual
git clone https://github.com/joselobelo/saber11-spark-bigdata.git
cd saber11-spark-bigdata

# 1) Descargar el conjunto de datos desde la fuente original (2019-2022, ~2,2 M filas)
python3 datos/descargar_saber11.py -o datos/saber11_2019_2022.csv

# 2) Subir el archivo a HDFS (HDFS debe estar iniciado: start-dfs.sh)
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put -f datos/saber11_2019_2022.csv /Tarea3/
hdfs dfs -ls -h /Tarea3

# 3) Ejecutar la aplicación de Spark
spark-submit --driver-memory 2g batch/saber11_batch.py \
    --input hdfs://localhost:9000/Tarea3/saber11_2019_2022.csv \
    --output salidas

# Alternativa: los pasos 1 a 3 en un solo comando
bash scripts/ejecutar_batch.sh
```

**Ver los resultados.** La consola muestra cada tabla. Los gráficos y el reporte quedan en `salidas/`. Como la máquina virtual no tiene entorno gráfico, se pueden abrir desde el navegador del equipo anfitrión:

```bash
cd salidas && python3 -m http.server 8000
# En el navegador del equipo anfitrión: http://<IP-de-la-VM>:8000/reporte.html
```

Mientras el trabajo corre, la interfaz de Spark está en `http://<IP-de-la-VM>:4040` (Jobs, Stages, Storage, Environment, Executors).

**Prueba rápida sin descargar todo:** `python3 datos/descargar_saber11.py --max-filas 100000 -o datos/muestra.csv` y luego `--input file://$PWD/datos/muestra.csv`.

## 8. Ejecución del procesamiento en tiempo real (Anexo 3)

```bash
# Terminal 1: ZooKeeper y Kafka (Kafka 3.9.2 instalado en /opt/Kafka según el Anexo 3)
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions 1 --topic sensor_data

# Terminal 1: productor (un evento JSON por segundo)
python3 streaming/kafka_producer.py

# Terminal 2: consumidor del Anexo 3
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    streaming/spark_streaming_consumer.py

# Terminal 2 (opcional): consumidor extendido con conteos, mín/máx y alertas
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    streaming/spark_streaming_consumer_ext.py

# Terminal 3 (opcional): gráfico que se actualiza cada 15 s
python3 streaming/graficar_streaming.py --cada 15
```

El consumidor extendido también puede probarse sin Kafka: `spark-submit streaming/spark_streaming_consumer_ext.py --sin-kafka --segundos 90`.

## 9. Pruebas y depuración

| Prueba | Resultado |
|---|---|
| `saber11_batch.py` de principio a fin con un CSV sintético de 95.996 filas (`pruebas/generar_datos_prueba.py`), PySpark 3.5.3 | Termina sin errores; detecta y elimina los duplicados, unifica `BOGOTA`/`BOGOTÁ`, genera 8 gráficos, 15 tablas y el reporte |
| `descargar_saber11.py` contra un servidor simulado con paginación | El archivo descargado es idéntico al original; respeta `--max-filas` |
| Formato de la URL de descarga contra la API real de datos.gov.co | Responde CSV con encabezado y paginación por `$offset` |
| `spark_streaming_consumer_ext.py --sin-kafka` durante 75 s | Calcula ventanas por sensor, alertas y escribe `ventanas.csv`; `graficar_streaming.py` genera el gráfico |
| Esquema del Anexo 3 (`timestamp` entero → `TimestampType`) | `from_json` convierte correctamente los segundos de época |

Errores frecuentes:

| Síntoma | Causa probable y solución |
|---|---|
| `Connection refused: localhost:9000` | HDFS no está iniciado: `start-dfs.sh`. Verifique el puerto con `hdfs getconf -confKey fs.defaultFS` |
| `java.lang.OutOfMemoryError` | Aumente `--driver-memory` (2g o 3g) o pruebe primero con `--max-filas` |
| `Name node is in safe mode` | `hdfs dfsadmin -safemode leave` |
| `No module named matplotlib` | `pip3 install matplotlib` (sin él, el análisis corre y solo se omiten los gráficos) |
| El consumidor no muestra datos | Inicie primero el productor y verifique el topic: `/opt/Kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092` |

## 10. Resultados

Los resultados se generan al ejecutar el código sobre los datos reales en `salidas/`. Después de la ejecución, agregue al repositorio los gráficos obtenidos:

```bash
git add salidas/graficos/*.png salidas/tablas/*.csv salidas/reporte.html
git commit -m "Resultados del procesamiento en batch"
git push
```

Video de demostración: https://youtu.be/S3rr41EvgX8

## 11. Evidencias de ejecución

Ejecución verificada el **29 de octubre de 2026** sobre el entorno Linux de la máquina del autor.

| Parámetro | Valor |
|---|---|
| Motor | Apache Spark 3.5.1 (PySpark), modo `local[4]` |
| Java | OpenJDK 11.0.32.1 |
| Recursos | 4 núcleos · 2 GB de memoria para el driver |
| Entrada del batch | `datos/prueba_saber11.csv` — 320.298 filas, 21 columnas (63 MB) |
| Tiempo total del batch | 21,3 s · `EXIT_CODE=0` |
| Streaming | 13 micro-lotes · 3 ventanas de 1 minuto × 10 sensores · `EXIT_CODE=0` |

### Qué se ejecutó y con qué datos

**Importante:** los datos de entrada del batch son **sintéticos**, generados con
`pruebas/generar_datos_prueba.py`. Reproducen exactamente el esquema de 21 columnas de la
fuente del ICFES y sus problemas de calidad (filas duplicadas en los periodos 20194 y 20224,
`BOGOTA` y `BOGOTÁ`, nulos, `Sin Estrato`, `No sabe`), pero **sus valores son aleatorios**.
Por eso las cifras de los gráficos de esta ejecución **no son resultados reales** y no
sustentan ninguna conclusión sobre las brechas educativas: solo demuestran que la aplicación
corre de principio a fin. Los resultados reales se obtienen ejecutando los mismos comandos
contra el archivo descargado con `datos/descargar_saber11.py`.

El motivo es que, en el entorno donde se ejecutó, la lista de dominios permitidos bloquea
`www.datos.gov.co`, de modo que no fue posible descargar el conjunto original.

Por la misma razón (bloqueo de `repo1.maven.org`, `downloads.apache.org` y
`archive.apache.org`) no se pudo instalar el broker de Kafka ni el conector
`spark-sql-kafka-0-10`. La parte de tiempo real se ejecutó con el modo `--sin-kafka` del
propio `streaming/spark_streaming_consumer_ext.py`, que alimenta con la fuente `rate` de
Spark **el mismo pipeline** de Structured Streaming: `from_json` → `withWatermark` →
`window("1 minute")` → agregaciones por sensor → sinks `console` y `foreachBatch`/CSV.
El código de Kafka (`streaming/kafka_producer.py` y `streaming/spark_streaming_consumer.py`)
queda tal como lo pide el Anexo 3, para ejecutarlo en la máquina virtual del curso.

### Archivos de evidencia

| Archivo | Contenido |
|---|---|
| `evidencias/log_ejecucion_batch.txt` | Log completo y sin editar de `spark-submit` del procesamiento en batch |
| `evidencias/log_ejecucion_batch_p1..p5.png` | El mismo log en imagen, para pegar en el informe o la presentación |
| `evidencias/log_ejecucion_streaming.txt` | Log completo del consumidor de Structured Streaming |
| `evidencias/log_ejecucion_streaming_p1..p6.png` | El mismo log en imagen |
| `evidencias/capturas/captura_01..06_*.png` | Capturas de pantalla del equipo: Finder con las evidencias y los graficos, Vista Previa con los registros de ejecucion y el grafico de estrato, y el repositorio en GitHub |
| `evidencias/ventanas_streaming.csv` | Estado final de las ventanas de 1 minuto por sensor |
| `evidencias/streaming_sensores.png` | Gráfico de temperatura promedio por sensor y eventos por ventana |
| `salidas/graficos/g1..g8*.png` | Los 8 gráficos del análisis en batch |
| `salidas/tablas/*.csv` | Las 15 tablas de resultados |
| `salidas/reporte.html` | Reporte HTML con todo el análisis |

Comandos exactos que se ejecutaron:

```bash
# 1) Datos de prueba con el esquema de la fuente
python3 pruebas/generar_datos_prueba.py -n 200000 -o datos/prueba_saber11.csv

# 2) Procesamiento en batch
spark-submit --master local[4] --driver-memory 2g batch/saber11_batch.py \
    --input datos/prueba_saber11.csv --output salidas \
    --parquet salidas_parquet/saber11_limpio.parquet

# 3) Procesamiento en tiempo real (sin broker de Kafka)
spark-submit --master local[4] streaming/spark_streaming_consumer_ext.py \
    --sin-kafka --segundos 120

# 4) Gráfico del streaming
python3 streaming/graficar_streaming.py --carpeta salidas_streaming
```

## 12. Referencias

- Instituto Colombiano para la Evaluación de la Educación. (2023). *Resultados únicos Saber 11* [Conjunto de datos]. Datos Abiertos Colombia. https://www.datos.gov.co/d/kgxf-xxbe
- Apache Software Foundation. (2024). *Apache Spark 3.5.3 documentation*. https://spark.apache.org/docs/3.5.3/
- Apache Software Foundation. (2025). *Apache Kafka documentation*. https://kafka.apache.org/documentation/
- Universidad Nacional Abierta y a Distancia. (2026). *Anexo 3. Instructivo Spark Streaming y Kafka*. Curso Big Data 202016911.
