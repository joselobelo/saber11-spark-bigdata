#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Descarga el conjunto "Resultados únicos Saber 11" (ICFES) desde su fuente
original: la API SODA del portal Datos Abiertos Colombia (datos.gov.co).

    Conjunto de datos : kgxf-xxbe  ·  licencia CC BY-SA 4.0
    URL del conjunto  : https://www.datos.gov.co/d/kgxf-xxbe

Por defecto descarga los periodos 2019-2022 (periodo >= '20191', unos 2,2
millones de filas) con las 21 columnas que usa el análisis, en páginas de
50 000 filas ordenadas por el identificador interno (:id) para que la
paginación sea estable. Solo usa la biblioteca estándar de Python.

Uso:
    python3 descargar_saber11.py                       # 2019-2022 -> saber11_2019_2022.csv
    python3 descargar_saber11.py --desde 20142 -o saber11_2014_2022.csv
    python3 descargar_saber11.py --max-filas 100000    # prueba rápida

Opcional: exporte SODA_APP_TOKEN=<token> para evitar limitaciones de la API.
"""
import argparse
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "https://www.datos.gov.co/resource/kgxf-xxbe.csv"

COLUMNAS = [
    "periodo", "estu_consecutivo", "estu_genero",
    "cole_naturaleza", "cole_area_ubicacion", "cole_calendario", "cole_jornada",
    "cole_caracter", "cole_depto_ubicacion", "cole_mcpio_ubicacion",
    "fami_estratovivienda", "fami_educacionmadre", "fami_educacionpadre",
    "fami_tieneinternet", "fami_tienecomputador",
    "punt_lectura_critica", "punt_matematicas", "punt_sociales_ciudadanas",
    "punt_c_naturales", "punt_ingles", "punt_global",
]


def construir_url(base, desde, limite, offset):
    params = {
        "$select": ",".join(COLUMNAS),
        "$where": "periodo >= '{}'".format(desde),
        "$order": ":id",
        "$limit": str(limite),
        "$offset": str(offset),
    }
    return base + "?" + urllib.parse.urlencode(params)


def pedir(url, intentos=4):
    cabeceras = {"Accept": "text/csv"}
    token = os.environ.get("SODA_APP_TOKEN")
    if token:
        cabeceras["X-App-Token"] = token
    for intento in range(1, intentos + 1):
        try:
            req = urllib.request.Request(url, headers=cabeceras)
            with urllib.request.urlopen(req, timeout=180) as resp:
                return resp.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            espera = 5 * intento
            print("  ! intento {} fallido ({}); reintento en {} s".format(intento, exc, espera))
            time.sleep(espera)
    raise RuntimeError("No fue posible descargar la página: " + url)


def main():
    ap = argparse.ArgumentParser(description="Descarga Saber 11 desde datos.gov.co (API SODA)")
    ap.add_argument("-o", "--salida", default="saber11_2019_2022.csv")
    ap.add_argument("--desde", default="20191", help="periodo inicial, p. ej. 20191 o 20142")
    ap.add_argument("--pagina", type=int, default=50000, help="filas por solicitud")
    ap.add_argument("--max-filas", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--base-url", default=BASE_URL, help=argparse.SUPPRESS)
    args = ap.parse_args()

    inicio = time.time()
    offset, total = 0, 0
    with open(args.salida, "w", encoding="utf-8", newline="") as f:
        while True:
            limite = args.pagina
            if args.max_filas:
                limite = min(limite, args.max_filas - total)
                if limite <= 0:
                    break
            texto = pedir(construir_url(args.base_url, args.desde, limite, offset))
            lineas = texto.splitlines(True)
            if not lineas:
                break
            encabezado, filas = lineas[0], lineas[1:]
            if offset == 0:
                f.write(encabezado)
            if not filas:
                break
            f.writelines(filas)
            total += len(filas)
            offset += len(filas)
            mb = os.path.getsize(args.salida) / 1e6
            print("  {:>10,} filas descargadas  ({:,.1f} MB, {:,.0f} s)".format(total, mb, time.time() - inicio))
            if len(filas) < limite:
                break
    print("Listo: {} filas en {} ({:.1f} MB) en {:.0f} s".format(
        total, args.salida, os.path.getsize(args.salida) / 1e6, time.time() - inicio))
    return 0


if __name__ == "__main__":
    sys.exit(main())
