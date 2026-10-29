#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD 202016911)
Procesamiento en batch con Apache Spark (PySpark) del conjunto
"Resultados únicos Saber 11" (ICFES, datos.gov.co, id kgxf-xxbe).

Problema: ¿qué tan grandes son las brechas en el puntaje global de Saber 11
según el estrato socioeconómico, la naturaleza y la zona del colegio, el
departamento, el acceso a internet y computador y la educación de la madre?

Etapas:
  1. Carga del CSV desde HDFS (o desde un archivo local).
  2. Perfil de calidad: filas, columnas, nulos y duplicados.
  3. Limpieza y transformación con DataFrames.
  4. Análisis exploratorio (EDA) con DataFrames.
  5. Operaciones equivalentes con RDD (map, filter, flatMap, groupByKey,
     reduceByKey, collect, count, take, reduce).
  6. Visualización: gráficos PNG, tablas CSV y un reporte HTML.

Ejecución en la máquina virtual (ver README.md):
  spark-submit --driver-memory 2g batch/saber11_batch.py \
      --input hdfs://localhost:9000/Tarea3/saber11_2019_2022.csv --output salidas
"""
import argparse
import csv
import os
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

AREAS = [
    ("punt_lectura_critica", "Lectura crítica"),
    ("punt_matematicas", "Matemáticas"),
    ("punt_sociales_ciudadanas", "Sociales y ciudadanas"),
    ("punt_c_naturales", "Ciencias naturales"),
    ("punt_ingles", "Inglés"),
]
ORDEN_EDUCACION = [
    "Ninguno", "Primaria incompleta", "Primaria completa",
    "Secundaria (Bachillerato) incompleta", "Secundaria (Bachillerato) completa",
    "Técnica o tecnológica incompleta", "Técnica o tecnológica completa",
    "Educación profesional incompleta", "Educación profesional completa", "Postgrado",
]
MIN_ESTUDIANTES_DEPTO = 1000   # departamentos con menos registros no entran al ranking

T0 = time.time()


def titulo(texto):
    print("\n" + "=" * 78)
    print("  {}   [t = {:.1f} s]".format(texto, time.time() - T0))
    print("=" * 78)


def guardar_csv(ruta, encabezado, filas):
    with open(ruta, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(encabezado)
        for fila in filas:
            w.writerow(fila)


def r2(x):
    return None if x is None else round(float(x), 2)


# --------------------------------------------------------------------------- 1
def cargar(spark, ruta):
    titulo("1. CARGA DEL CONJUNTO DE DATOS")
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)      # todo llega como texto; se tipa en la limpieza
          .option("encoding", "UTF-8")
          .csv(ruta))
    print("Fuente:", ruta)
    print("Particiones de lectura:", df.rdd.getNumPartitions())
    df.printSchema()
    return df


# --------------------------------------------------------------------------- 2
def perfil_calidad(df, salida):
    titulo("2. PERFIL DE CALIDAD DE LOS DATOS")
    filas = df.count()
    unicos = df.select("estu_consecutivo").distinct().count()
    print("Filas brutas: {:,}   Columnas: {}".format(filas, len(df.columns)))
    print("Estudiantes únicos (estu_consecutivo): {:,}".format(unicos))
    print("Filas duplicadas: {:,} ({:.1f} %)".format(filas - unicos, 100.0 * (filas - unicos) / max(filas, 1)))

    print("\nFilas y estudiantes únicos por periodo:")
    por_periodo = (df.groupBy("periodo")
                   .agg(F.count("*").alias("filas"), F.countDistinct("estu_consecutivo").alias("unicos"))
                   .withColumn("duplicadas", F.col("filas") - F.col("unicos"))
                   .orderBy("periodo"))
    por_periodo.show(truncate=False)

    nulos = df.select([F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
                       for c in df.columns]).collect()[0].asDict()
    print("Valores nulos o vacíos por columna:")
    for c in df.columns:
        print("  {:<28} {:>10,}  ({:5.2f} %)".format(c, nulos[c], 100.0 * nulos[c] / max(filas, 1)))
    guardar_csv(os.path.join(salida, "tablas", "00_nulos_por_columna.csv"),
                ["columna", "nulos", "porcentaje"],
                [[c, nulos[c], r2(100.0 * nulos[c] / max(filas, 1))] for c in df.columns])
    guardar_csv(os.path.join(salida, "tablas", "00_duplicados_por_periodo.csv"),
                ["periodo", "filas", "unicos", "duplicadas"],
                [list(r) for r in por_periodo.collect()])
    return {"filas_brutas": filas, "unicos": unicos}


# --------------------------------------------------------------------------- 3
def sin_tildes(col):
    return F.translate(F.upper(F.trim(col)), "ÁÉÍÓÚÜ", "AEIOUU")


def limpiar(df, perfil):
    titulo("3. LIMPIEZA Y TRANSFORMACIÓN")
    n0, n1 = perfil["filas_brutas"], perfil["unicos"]

    # 3.1 Duplicados: el mismo estudiante aparece repetido en los periodos 20194 y 20224
    df = df.dropDuplicates(["estu_consecutivo"])
    print("3.1 Duplicados eliminados (dropDuplicates por estu_consecutivo): {:,}".format(n0 - n1))

    # 3.2 Tipos: los puntajes llegan como texto
    for c, _ in AREAS + [("punt_global", "Global")]:
        df = df.withColumn(c, F.col(c).cast(T.IntegerType()))

    # 3.3 Filas sin puntaje global o fuera del rango 0-500 no sirven al análisis
    df = df.filter(F.col("punt_global").isNotNull() & F.col("punt_global").between(0, 500))
    n2 = df.count()
    print("3.3 Filas sin puntaje global válido eliminadas: {:,}".format(n1 - n2))

    # 3.4 Normalización de texto (mayúsculas, sin tildes, sin espacios sobrantes)
    #     Corrige, p. ej., 'BOGOTA' y 'BOGOTÁ' como dos departamentos distintos
    df = (df
          .withColumn("departamento", sin_tildes(F.col("cole_depto_ubicacion")))
          .withColumn("naturaleza", F.upper(F.trim(F.col("cole_naturaleza"))))
          .withColumn("zona", F.upper(F.trim(F.col("cole_area_ubicacion"))))
          .withColumn("calendario", F.upper(F.trim(F.col("cole_calendario")))))

    # 3.5 Variables derivadas
    estrato_txt = F.trim(F.col("fami_estratovivienda"))
    df = (df
          .withColumn("anio", F.substring("periodo", 1, 4).cast(T.IntegerType()))
          .withColumn("estrato",
                      F.when(estrato_txt.rlike("^Estrato [1-6]$"), F.substring(estrato_txt, 9, 1).cast(T.IntegerType()))
                       .when(estrato_txt == "Sin Estrato", F.lit(0)))
          .withColumn("internet", F.when(F.trim(F.col("fami_tieneinternet")) == "Si", "Con internet")
                                   .when(F.trim(F.col("fami_tieneinternet")) == "No", "Sin internet"))
          .withColumn("computador", F.when(F.trim(F.col("fami_tienecomputador")) == "Si", "Con computador")
                                     .when(F.trim(F.col("fami_tienecomputador")) == "No", "Sin computador"))
          .withColumn("educacion_madre",
                      F.when(F.col("fami_educacionmadre").isin(ORDEN_EDUCACION), F.col("fami_educacionmadre")))
          .withColumn("nivel_desempeno",   # segmentación propia del análisis, no niveles oficiales del ICFES
                      F.when(F.col("punt_global") < 200, "Bajo (<200)")
                       .when(F.col("punt_global") < 300, "Medio (200-299)")
                       .when(F.col("punt_global") < 400, "Alto (300-399)")
                       .otherwise("Superior (>=400)")))

    columnas = ["periodo", "anio", "calendario", "estu_consecutivo", "estu_genero", "naturaleza", "zona",
                "departamento", "estrato", "internet", "computador", "educacion_madre",
                "nivel_desempeno"] + [c for c, _ in AREAS] + ["punt_global"]
    df = df.select(*columnas)
    print("3.5 Columnas derivadas: anio, estrato (0 = sin estrato), internet, computador,")
    print("    educacion_madre (sin 'No sabe'/'No Aplica'), nivel_desempeno")
    print("Registros limpios: {:,}".format(n2))
    df.show(5, truncate=False)
    return df


# --------------------------------------------------------------------------- 4
def agrupar(df, columna, salida, nombre, filtro=None, orden=None):
    base = df if filtro is None else df.filter(filtro)
    res = (base.groupBy(columna)
           .agg(F.count("*").alias("estudiantes"),
                F.round(F.avg("punt_global"), 2).alias("promedio_global"),
                F.round(F.stddev("punt_global"), 2).alias("desv_estandar"))
           .orderBy(orden if orden is not None else columna))
    res.show(60, truncate=False)
    filas = res.collect()
    guardar_csv(os.path.join(salida, "tablas", nombre + ".csv"),
                [columna, "estudiantes", "promedio_global", "desv_estandar"], [list(r) for r in filas])
    return filas


def eda(df, salida):
    titulo("4. ANÁLISIS EXPLORATORIO (DataFrames)")
    R = {}

    print("4.1 Estadística descriptiva de los puntajes")
    desc = df.select([c for c, _ in AREAS] + ["punt_global"]).describe()
    desc.show(truncate=False)
    guardar_csv(os.path.join(salida, "tablas", "01_descriptiva.csv"), desc.columns, [list(r) for r in desc.collect()])
    q = df.approxQuantile("punt_global", [0.25, 0.5, 0.75], 0.001)
    print("Cuartiles aproximados del puntaje global: Q1={:.0f}  mediana={:.0f}  Q3={:.0f}".format(*q))
    R["cuartiles"] = q

    print("4.2 Distribución del puntaje global (intervalos de 10 puntos)")
    hist = (df.withColumn("intervalo", (F.floor(F.col("punt_global") / 10) * 10).cast("int"))
            .groupBy("intervalo").count().orderBy("intervalo").collect())
    R["hist"] = [(r["intervalo"], r["count"]) for r in hist]
    guardar_csv(os.path.join(salida, "tablas", "02_histograma.csv"), ["intervalo", "estudiantes"], R["hist"])

    print("4.3 Puntaje global por año y calendario")
    R["anio"] = agrupar(df, "anio", salida, "03_por_anio")
    agrupar(df, "calendario", salida, "03b_por_calendario")

    print("4.4 Puntaje global por estrato (0 = sin estrato)")
    R["estrato"] = agrupar(df, "estrato", salida, "04_por_estrato", F.col("estrato").isNotNull())

    print("4.5 Puntaje global por naturaleza y zona del colegio")
    nz = (df.filter(F.col("naturaleza").isNotNull() & F.col("zona").isNotNull())
          .groupBy("naturaleza", "zona")
          .agg(F.count("*").alias("estudiantes"), F.round(F.avg("punt_global"), 2).alias("promedio_global"))
          .orderBy("naturaleza", "zona"))
    nz.show(truncate=False)
    R["nat_zona"] = nz.collect()
    guardar_csv(os.path.join(salida, "tablas", "05_naturaleza_zona.csv"), nz.columns, [list(r) for r in R["nat_zona"]])

    print("4.6 Ranking de departamentos (mínimo {} estudiantes)".format(MIN_ESTUDIANTES_DEPTO))
    dep = (df.filter(F.col("departamento").isNotNull())
           .groupBy("departamento")
           .agg(F.count("*").alias("estudiantes"), F.round(F.avg("punt_global"), 2).alias("promedio_global"))
           .filter(F.col("estudiantes") >= MIN_ESTUDIANTES_DEPTO)
           .orderBy(F.desc("promedio_global")))
    dep.show(40, truncate=False)
    R["depto"] = dep.collect()
    guardar_csv(os.path.join(salida, "tablas", "06_ranking_departamentos.csv"), dep.columns, [list(r) for r in R["depto"]])

    print("4.7 Acceso a internet y computador")
    ic = (df.filter(F.col("internet").isNotNull() & F.col("computador").isNotNull())
          .groupBy("internet", "computador")
          .agg(F.count("*").alias("estudiantes"), F.round(F.avg("punt_global"), 2).alias("promedio_global"))
          .orderBy("internet", "computador"))
    ic.show(truncate=False)
    R["tic"] = ic.collect()
    guardar_csv(os.path.join(salida, "tablas", "07_internet_computador.csv"), ic.columns, [list(r) for r in R["tic"]])

    print("4.8 Educación de la madre")
    em = agrupar(df, "educacion_madre", salida, "08_educacion_madre", F.col("educacion_madre").isNotNull())
    orden = {v: i for i, v in enumerate(ORDEN_EDUCACION)}
    R["madre"] = sorted(em, key=lambda r: orden[r["educacion_madre"]])

    print("4.9 Género del estudiante")
    agrupar(df, "estu_genero", salida, "09_genero", F.col("estu_genero").isin("F", "M"))

    print("4.10 Correlación de Pearson entre áreas")
    cols = [c for c, _ in AREAS]
    base = df.select(cols).dropna()
    corr = [[1.0 if a == b else base.stat.corr(a, b) for b in cols] for a in cols]
    for i, a in enumerate(cols):
        print("  {:<26}".format(a) + " ".join("{:6.3f}".format(v) for v in corr[i]))
    R["corr"] = corr
    guardar_csv(os.path.join(salida, "tablas", "10_correlaciones.csv"), [""] + cols,
                [[cols[i]] + [r2(v) for v in corr[i]] for i in range(len(cols))])

    print("4.11 Niveles de desempeño por naturaleza del colegio (%)")
    niv = (df.filter(F.col("naturaleza").isNotNull())
           .groupBy("naturaleza").pivot("nivel_desempeno",
                                        ["Bajo (<200)", "Medio (200-299)", "Alto (300-399)", "Superior (>=400)"])
           .count().na.fill(0))
    total = sum(F.col(c) for c in niv.columns[1:])
    niv = niv.select("naturaleza", *[F.round(100 * F.col(c) / total, 2).alias(c) for c in niv.columns[1:]])
    niv.show(truncate=False)
    guardar_csv(os.path.join(salida, "tablas", "11_niveles_naturaleza.csv"), niv.columns, [list(r) for r in niv.collect()])
    return R


# --------------------------------------------------------------------------- 5
def operaciones_rdd(df, salida):
    titulo("5. OPERACIONES CON RDD (transformaciones y acciones)")
    rdd = df.select("departamento", "naturaleza", *[c for c, _ in AREAS], "punt_global").rdd

    # count (acción)
    print("count() -> registros en el RDD: {:,}".format(rdd.count()))

    # filter (transformación) + count (acción)
    sobresalientes = rdd.filter(lambda r: r["punt_global"] >= 400)
    print("filter(punt_global >= 400).count() -> {:,} estudiantes".format(sobresalientes.count()))

    # map + reduceByKey (transformaciones) + take (acción): promedio por departamento
    prom_depto = (rdd.filter(lambda r: r["departamento"] is not None)
                  .map(lambda r: (r["departamento"], (r["punt_global"], 1)))
                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                  .filter(lambda kv: kv[1][1] >= MIN_ESTUDIANTES_DEPTO)
                  .mapValues(lambda s: round(s[0] / s[1], 2))
                  .sortBy(lambda kv: kv[1], ascending=False))
    print("map + reduceByKey + take(5) -> 5 departamentos con mayor promedio:")
    for d, p in prom_depto.take(5):
        print("   {:<20} {:>7}".format(d, p))

    # flatMap: cada estudiante se convierte en 5 pares (área, puntaje)
    nombres = dict(AREAS)
    por_area = (rdd.flatMap(lambda r: [(nombres[c], (r[c], 1)) for c in nombres if r[c] is not None])
                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                .mapValues(lambda s: round(s[0] / s[1], 2)))
    print("flatMap + reduceByKey + collect() -> promedio por área:")
    areas = sorted(por_area.collect(), key=lambda kv: -kv[1])
    for a, p in areas:
        print("   {:<24} {:>7}".format(a, p))

    # groupByKey vs reduceByKey: máximo por naturaleza sobre una muestra del 1 %
    muestra = (rdd.sample(False, 0.01, seed=42)
               .filter(lambda r: r["naturaleza"] is not None)
               .map(lambda r: (r["naturaleza"], r["punt_global"])))
    con_group = sorted(muestra.groupByKey().mapValues(max).collect())
    con_reduce = sorted(muestra.reduceByKey(max).collect())
    print("groupByKey().mapValues(max) (muestra 1 %) ->", con_group)
    print("reduceByKey(max)           (muestra 1 %) ->", con_reduce,
          "  (mismo resultado, menos datos en el shuffle)")

    # reduce (acción): promedio nacional calculado con RDD
    suma = rdd.map(lambda r: r["punt_global"]).reduce(lambda a, b: a + b)
    n = rdd.count()
    print("reduce(+) -> suma = {:,}; promedio nacional = {:.2f}".format(suma, suma / n))
    guardar_csv(os.path.join(salida, "tablas", "12_rdd_promedio_por_area.csv"), ["area", "promedio"], areas)
    return {"areas": areas, "promedio_rdd": suma / n, "sobresalientes": sobresalientes.count()}


# --------------------------------------------------------------------------- 6
def graficos(R, rdd_res, salida):
    titulo("6. VISUALIZACIÓN DE RESULTADOS")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib no está instalado: se omiten los gráficos (pip3 install matplotlib).")
        return []
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                         "axes.titleweight": "bold", "axes.titlesize": 12})
    AZUL, VERDE, NARANJA, GRIS = "#1F3A5C", "#3E8E63", "#D9822B", "#5F6B76"
    gdir = os.path.join(salida, "graficos")
    hechos = []

    def fin(fig, nombre, titulo_):
        ruta = os.path.join(gdir, nombre)
        fig.tight_layout()
        fig.savefig(ruta, dpi=150)
        plt.close(fig)
        hechos.append((nombre, titulo_))
        print("  ->", ruta)

    # G1 histograma
    fig, ax = plt.subplots(figsize=(9, 4.6))
    xs = [h[0] for h in R["hist"]]
    ax.bar(xs, [h[1] for h in R["hist"]], width=9, color=AZUL)
    for v, lab in zip(R["cuartiles"], ["Q1", "Mediana", "Q3"]):
        ax.axvline(v, color=NARANJA, ls="--", lw=1.2)
        ax.text(v, ax.get_ylim()[1] * 0.95, " " + lab, color=NARANJA, fontsize=9)
    ax.set_xlabel("Puntaje global (intervalos de 10 puntos)")
    ax.set_ylabel("Estudiantes")
    ax.set_title("Distribución del puntaje global Saber 11")
    fin(fig, "g1_distribucion_puntaje_global.png", "Distribución del puntaje global")

    # G2 estrato
    est = R["estrato"]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    etiquetas = ["Sin estrato" if r["estrato"] == 0 else "Estrato {}".format(r["estrato"]) for r in est]
    vals = [r["promedio_global"] for r in est]
    barras = ax.bar(etiquetas, vals, color=[GRIS if r["estrato"] == 0 else AZUL for r in est])
    ax.bar_label(barras, fmt="%.1f", padding=3, fontsize=9)
    ax.set_ylim(min(vals) * 0.85, max(vals) * 1.06)
    ax.set_ylabel("Puntaje global promedio")
    ax.set_title("Puntaje global promedio por estrato socioeconómico")
    fin(fig, "g2_puntaje_por_estrato.png", "Puntaje promedio por estrato")

    # G3 naturaleza x zona
    nz = R["nat_zona"]
    zonas = sorted({r["zona"] for r in nz})
    nats = sorted({r["naturaleza"] for r in nz})
    fig, ax = plt.subplots(figsize=(8, 4.6))
    ancho = 0.38
    for i, z in enumerate(zonas):
        ys = [next((r["promedio_global"] for r in nz if r["naturaleza"] == n and r["zona"] == z), 0) for n in nats]
        b = ax.bar([k + (i - 0.5) * ancho for k in range(len(nats))], ys, ancho, label=z.title(),
                   color=[AZUL, VERDE][i % 2])
        ax.bar_label(b, fmt="%.1f", padding=3, fontsize=9)
    ax.set_xticks(range(len(nats)))
    ax.set_xticklabels([n.title() for n in nats])
    ax.set_ylim(150, None)
    ax.set_ylabel("Puntaje global promedio")
    ax.set_title("Puntaje global por naturaleza y zona del colegio")
    ax.legend(frameon=False, title="Zona", loc="upper left", bbox_to_anchor=(1.0, 1.0))
    fin(fig, "g3_naturaleza_zona.png", "Naturaleza y zona del colegio")

    # G4 departamentos
    dep = R["depto"]
    fig, ax = plt.subplots(figsize=(9, max(5, 0.28 * len(dep))))
    nombres = [r["departamento"].title() for r in dep][::-1]
    vals = [r["promedio_global"] for r in dep][::-1]
    media = sum(vals) / len(vals) if vals else 0
    ax.barh(nombres, vals, color=[VERDE if v >= media else NARANJA for v in vals])
    ax.axvline(media, color=GRIS, ls="--", lw=1)
    ax.set_xlim(min(vals) * 0.9 if vals else 0, max(vals) * 1.03 if vals else 1)
    ax.set_xlabel("Puntaje global promedio (línea: media de departamentos)")
    ax.set_title("Ranking de departamentos por puntaje global")
    fin(fig, "g4_ranking_departamentos.png", "Ranking de departamentos")

    # G5 internet x computador
    tic = R["tic"]
    fig, ax = plt.subplots(figsize=(8, 4.6))
    et = ["{}\n{}".format(r["internet"], r["computador"].lower()) for r in tic]
    b = ax.bar(et, [r["promedio_global"] for r in tic], color=[VERDE, GRIS, GRIS, NARANJA][:len(tic)])
    ax.bar_label(b, fmt="%.1f", padding=3, fontsize=9)
    ax.set_ylim(150, None)
    ax.set_ylabel("Puntaje global promedio")
    ax.set_title("Acceso a internet y computador en el hogar")
    fin(fig, "g5_internet_computador.png", "Internet y computador")

    # G6 educación de la madre
    em = R["madre"]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh([r["educacion_madre"] for r in em][::-1], [r["promedio_global"] for r in em][::-1], color=AZUL)
    ax.set_xlim(150, None)
    ax.set_xlabel("Puntaje global promedio")
    ax.set_title("Puntaje global según la educación de la madre")
    fin(fig, "g6_educacion_madre.png", "Educación de la madre")

    # G7 correlaciones
    corr = R["corr"]
    nombres_a = [n for _, n in AREAS]
    fig, ax = plt.subplots(figsize=(7, 5.6))
    im = ax.imshow(corr, cmap="Blues", vmin=0, vmax=1)
    ax.set_xticks(range(len(nombres_a)))
    ax.set_xticklabels(nombres_a, rotation=30, ha="right")
    ax.set_yticks(range(len(nombres_a)))
    ax.set_yticklabels(nombres_a)
    for i in range(len(corr)):
        for j in range(len(corr)):
            ax.text(j, i, "{:.2f}".format(corr[i][j]), ha="center", va="center",
                    color="white" if corr[i][j] > 0.6 else "black", fontsize=9)
    fig.colorbar(im, ax=ax, shrink=0.8)
    ax.set_title("Correlación de Pearson entre áreas")
    fin(fig, "g7_correlacion_areas.png", "Correlación entre áreas")

    # G8 promedio por área (resultado del RDD con flatMap)
    fig, ax = plt.subplots(figsize=(8, 4.4))
    b = ax.bar([a for a, _ in rdd_res["areas"]], [p for _, p in rdd_res["areas"]], color=VERDE)
    ax.bar_label(b, fmt="%.1f", padding=3, fontsize=9)
    ax.set_ylabel("Puntaje promedio (escala 0-100)")
    ax.set_title("Promedio por área (calculado con flatMap + reduceByKey)")
    plt.setp(ax.get_xticklabels(), rotation=15, ha="right")
    fin(fig, "g8_promedio_por_area_rdd.png", "Promedio por área (RDD)")
    return hechos


def reporte_html(salida, perfil, R, rdd_res, hechos):
    filas = "".join("<tr><td>{}</td><td>{:,}</td><td>{}</td></tr>".format(
        "Sin estrato" if r["estrato"] == 0 else "Estrato {}".format(r["estrato"]), r["estudiantes"], r["promedio_global"])
        for r in R["estrato"])
    imgs = "".join('<figure><img src="graficos/{0}" alt="{1}"><figcaption>{1}</figcaption></figure>'.format(n, t)
                   for n, t in hechos)
    html = """<!doctype html><html lang="es"><meta charset="utf-8">
<title>Saber 11 · Resultados del procesamiento en batch</title>
<style>body{{font-family:Arial,sans-serif;max-width:1100px;margin:24px auto;padding:0 16px;color:#1d2733}}
h1{{color:#1F3A5C}} .kpi{{display:flex;gap:12px;flex-wrap:wrap}} .kpi div{{background:#EEF3F8;border-radius:8px;padding:12px 16px;min-width:180px}}
.kpi b{{display:block;font-size:24px;color:#1F3A5C}} table{{border-collapse:collapse}} td,th{{border:1px solid #ccd;padding:4px 10px}}
figure{{margin:18px 0}} img{{max-width:100%;border:1px solid #e3e8ee}}</style>
<h1>Saber 11 (2019-2022) · Procesamiento en batch con Apache Spark</h1>
<p>Fuente: ICFES, Resultados únicos Saber 11, datos.gov.co (kgxf-xxbe). Generado por saber11_batch.py.</p>
<div class="kpi"><div><b>{fb:,}</b>filas brutas</div><div><b>{un:,}</b>estudiantes únicos</div>
<div><b>{pr:.1f}</b>puntaje global promedio</div><div><b>{so:,}</b>estudiantes con 400 o más</div></div>
<h2>Puntaje global por estrato</h2><table><tr><th>Estrato</th><th>Estudiantes</th><th>Promedio</th></tr>{filas}</table>
<h2>Gráficos</h2>{imgs}</html>""".format(fb=perfil["filas_brutas"], un=perfil["unicos"], pr=rdd_res["promedio_rdd"],
                                         so=rdd_res["sobresalientes"], filas=filas, imgs=imgs)
    ruta = os.path.join(salida, "reporte.html")
    with open(ruta, "w", encoding="utf-8") as f:
        f.write(html)
    print("  -> reporte HTML:", ruta)


def main():
    ap = argparse.ArgumentParser(description="Procesamiento batch de Saber 11 con PySpark")
    ap.add_argument("--input", default="hdfs://localhost:9000/Tarea3/saber11_2019_2022.csv",
                    help="ruta del CSV (hdfs://... o file:///...)")
    ap.add_argument("--output", default="salidas", help="carpeta local para gráficos y tablas")
    ap.add_argument("--parquet", default="", help="ruta opcional para guardar el DataFrame limpio en Parquet")
    args = ap.parse_args()

    os.makedirs(os.path.join(args.output, "graficos"), exist_ok=True)
    os.makedirs(os.path.join(args.output, "tablas"), exist_ok=True)

    spark = (SparkSession.builder.appName("Saber11-Batch-Tarea3")
             .config("spark.sql.shuffle.partitions", "8")   # pocas particiones de mezcla: VM de 1 nodo
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print("Spark {} · master {}".format(spark.version, spark.sparkContext.master))

    crudo = cargar(spark, args.input)
    perfil = perfil_calidad(crudo, args.output)
    limpio = limpiar(crudo, perfil).persist(StorageLevel.MEMORY_AND_DISK)
    limpio.count()   # materializa la caché: las consultas siguientes leen de memoria
    if args.parquet:
        limpio.write.mode("overwrite").parquet(args.parquet)
        print("DataFrame limpio guardado en Parquet:", args.parquet)
    R = eda(limpio, args.output)
    rdd_res = operaciones_rdd(limpio, args.output)
    hechos = graficos(R, rdd_res, args.output)
    reporte_html(args.output, perfil, R, rdd_res, hechos)

    titulo("FIN · tiempo total {:.1f} s · Spark UI: http://<IP-de-la-VM>:4040".format(time.time() - T0))
    limpio.unpersist()
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
