#!/usr/bin/env bash
# Descarga Saber 11 (2019-2022), lo sube a HDFS y ejecuta el procesamiento en batch.
# Uso: bash scripts/ejecutar_batch.sh            (desde la raíz del repositorio)
set -euo pipefail
CSV="datos/saber11_2019_2022.csv"
HDFS_DIR="/Tarea3"
HDFS_URI="${HDFS_URI:-hdfs://localhost:9000}"

if [ ! -s "$CSV" ]; then
  echo ">> Descargando datos desde datos.gov.co ..."
  python3 datos/descargar_saber11.py -o "$CSV"
fi
echo ">> Archivo local: $(du -h "$CSV" | cut -f1)  ·  $(($(wc -l < "$CSV") - 1)) filas"

echo ">> Subiendo a HDFS $HDFS_DIR ..."
hdfs dfs -mkdir -p "$HDFS_DIR"
hdfs dfs -put -f "$CSV" "$HDFS_DIR/"
hdfs dfs -ls -h "$HDFS_DIR"

echo ">> Ejecutando Spark ..."
spark-submit --driver-memory "${DRIVER_MEM:-2g}" batch/saber11_batch.py \
  --input "$HDFS_URI$HDFS_DIR/$(basename "$CSV")" --output salidas

echo ">> Listo. Resultados en ./salidas  (ver: cd salidas && python3 -m http.server 8000)"
