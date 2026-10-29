#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SOLO PARA PRUEBAS DEL CÓDIGO. Genera un CSV SINTÉTICO con el mismo encabezado
y las mismas categorías que entrega la API de datos.gov.co para Saber 11,
incluidos sus problemas de calidad (filas duplicadas en 20194 y 20224,
'BOGOTA' y 'BOGOTÁ', nulos, 'Sin Estrato', 'No sabe').

Los valores son aleatorios: NO son resultados reales y no deben usarse
para conclusiones. Sirve para verificar que saber11_batch.py corre de
principio a fin sin necesidad de descargar el conjunto completo.

Uso: python3 generar_datos_prueba.py -n 50000 -o prueba_saber11.csv
"""
import argparse
import csv
import random

COLS = ["periodo", "estu_consecutivo", "estu_genero", "cole_naturaleza", "cole_area_ubicacion",
        "cole_calendario", "cole_jornada", "cole_caracter", "cole_depto_ubicacion", "cole_mcpio_ubicacion",
        "fami_estratovivienda", "fami_educacionmadre", "fami_educacionpadre", "fami_tieneinternet",
        "fami_tienecomputador", "punt_lectura_critica", "punt_matematicas", "punt_sociales_ciudadanas",
        "punt_c_naturales", "punt_ingles", "punt_global"]
DEPTOS = ["ANTIOQUIA", "ATLANTICO", "BOGOTÁ", "BOGOTA", "BOLIVAR", "BOYACA", "CALDAS", "CAUCA", "CESAR",
          "CORDOBA", "CUNDINAMARCA", "HUILA", "LA GUAJIRA", "MAGDALENA", "META", "NARIÑO", "NORTE SANTANDER",
          "SANTANDER", "SUCRE", "TOLIMA", "VALLE", "CHOCO", "AMAZONAS"]
EDU = ["Ninguno", "Primaria incompleta", "Primaria completa", "Secundaria (Bachillerato) incompleta",
       "Secundaria (Bachillerato) completa", "Técnica o tecnológica incompleta", "Técnica o tecnológica completa",
       "Educación profesional incompleta", "Educación profesional completa", "Postgrado", "No sabe", "No Aplica", ""]
ESTRATOS = ["Estrato 1", "Estrato 2", "Estrato 3", "Estrato 4", "Estrato 5", "Estrato 6", "Sin Estrato", ""]


def fila(rng, periodo, k):
    est = rng.choice(ESTRATOS)
    nat = rng.choice(["OFICIAL", "OFICIAL", "OFICIAL", "NO OFICIAL"])
    base = rng.gauss(50, 10)
    areas = [max(0, min(100, int(rng.gauss(base, 8)))) for _ in range(5)]
    glob = int(round((areas[0] * 3 + areas[1] * 3 + areas[2] * 3 + areas[3] * 3 + areas[4]) / 13 * 5))
    return [periodo, "SB11{}{:07d}".format(periodo, k), rng.choice(["F", "M", "F", "M", ""]), nat,
            rng.choice(["URBANO", "URBANO", "RURAL"]), "A" if nat == "OFICIAL" else rng.choice(["A", "B", "OTRO"]),
            rng.choice(["MAÑANA", "TARDE", "COMPLETA", "UNICA"]), rng.choice(["ACADÉMICO", "TÉCNICO/ACADÉMICO"]),
            rng.choice(DEPTOS), "MUNICIPIO", est, rng.choice(EDU), rng.choice(EDU),
            rng.choice(["Si", "No", ""]), rng.choice(["Si", "No", ""])] + [str(a) for a in areas] + [str(glob)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=50000)
    ap.add_argument("-o", default="prueba_saber11.csv")
    a = ap.parse_args()
    rng = random.Random(7)
    with open(a.o, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(COLS)
        for k in range(a.n):
            periodo = rng.choice(["20191", "20194", "20194", "20194", "20201", "20211", "20221", "20224", "20224", "20224"])
            r = fila(rng, periodo, k)
            w.writerow(r)
            if periodo in ("20194", "20224"):
                w.writerow(r)   # duplicado, como en la fuente
    print("CSV sintético de prueba:", a.o)


if __name__ == "__main__":
    main()
