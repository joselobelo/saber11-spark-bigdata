# -*- coding: utf-8 -*-
"""
Visualiza los resultados del procesamiento en tiempo real.
Lee salidas_streaming/ventanas.csv (lo escribe spark_streaming_consumer_ext.py)
y genera salidas_streaming/streaming_sensores.png con:
  - temperatura promedio por sensor en cada ventana de 1 minuto
  - eventos recibidos por ventana

Uso:  python3 graficar_streaming.py            (una vez)
      python3 graficar_streaming.py --cada 15  (se actualiza cada 15 s; Ctrl+C para salir)
"""
import argparse
import csv
import os
import time
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def graficar(carpeta):
    ruta = os.path.join(carpeta, "ventanas.csv")
    if not os.path.exists(ruta):
        print("Aún no existe", ruta)
        return
    with open(ruta, encoding="utf-8") as f:
        filas = list(csv.DictReader(f))
    if not filas:
        print("Sin datos todavía")
        return
    ventanas = sorted({r["inicio"] for r in filas})
    temp = defaultdict(dict)
    eventos = defaultdict(int)
    for r in filas:
        temp[int(r["sensor_id"])][r["inicio"]] = float(r["temp_prom"])
        eventos[r["inicio"]] += int(r["eventos"])

    fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 7), gridspec_kw={"height_ratios": [2, 1]})
    for s in sorted(temp):
        xs = [v for v in ventanas if v in temp[s]]
        a1.plot(xs, [temp[s][v] for v in xs], marker="o", lw=1.4, label="Sensor {}".format(s))
    a1.axhline(28, color="#B5483B", ls="--", lw=1)
    a1.set_ylabel("Temperatura promedio (°C)")
    a1.set_title("Temperatura promedio por sensor y ventana de 1 minuto")
    a1.legend(ncol=5, fontsize=8, frameon=False)
    a2.bar(ventanas, [eventos[v] for v in ventanas], color="#2E86AB")
    a2.set_ylabel("Eventos")
    a2.set_xlabel("Inicio de la ventana (HH:mm)")
    for a in (a1, a2):
        a.spines["top"].set_visible(False)
        a.spines["right"].set_visible(False)
    fig.tight_layout()
    salida = os.path.join(carpeta, "streaming_sensores.png")
    fig.savefig(salida, dpi=130)
    plt.close(fig)
    print("Gráfico actualizado:", salida, "({} ventanas)".format(len(ventanas)))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--carpeta", default="salidas_streaming")
    ap.add_argument("--cada", type=int, default=0)
    a = ap.parse_args()
    if a.cada:
        while True:
            graficar(a.carpeta)
            time.sleep(a.cada)
    else:
        graficar(a.carpeta)
