from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .appName("AnalisisHurtosColombia") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

file_path = "hdfs://localhost:9000/Tarea3/hurtos.csv"

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

print("\n========== ESQUEMA DEL DATASET ==========\n")
df.printSchema()

print("\n========== TOTAL DE REGISTROS ==========\n")
print("Total de registros:", df.count())

print("\n========== PRIMEROS REGISTROS ==========\n")
df.select(
    "departamento",
    "municipio",
    "armas_medios",
    "fecha_hecho",
    "genero",
    "grupo_etario",
    "tipo_de_hurto",
    "cantidad"
).show(10, truncate=False)

# =========================
# LIMPIEZA Y TRANSFORMACION
# =========================

df_limpio = df.dropDuplicates()

df_limpio = df_limpio.dropna(
    subset=[
        "departamento",
        "municipio",
        "fecha_hecho",
        "tipo_de_hurto",
        "cantidad"
    ]
)

df_limpio = df_limpio.withColumn(
    "fecha",
    F.to_date(F.col("fecha_hecho"), "dd/MM/yyyy")
)

df_limpio = df_limpio.withColumn(
    "anio",
    F.year(F.col("fecha"))
)

print("\n========== REGISTROS DESPUES DE LIMPIEZA ==========\n")
print("Total de registros limpios:", df_limpio.count())

# =========================
# ANALISIS 1
# Hurtos por departamento
# =========================

print("\n========== TOP 10 DEPARTAMENTOS CON MAYOR CANTIDAD DE HURTOS ==========\n")

hurtos_departamento = df_limpio.groupBy("departamento") \
    .agg(F.sum("cantidad").alias("total_hurtos")) \
    .orderBy(F.desc("total_hurtos"))

hurtos_departamento.show(10, truncate=False)

# =========================
# ANALISIS 2
# Hurtos por municipio
# =========================

print("\n========== TOP 10 MUNICIPIOS CON MAYOR CANTIDAD DE HURTOS ==========\n")

hurtos_municipio = df_limpio.groupBy("municipio") \
    .agg(F.sum("cantidad").alias("total_hurtos")) \
    .orderBy(F.desc("total_hurtos"))

hurtos_municipio.show(10, truncate=False)

# =========================
# ANALISIS 3
# Tipos de hurto
# =========================

print("\n========== TIPOS DE HURTO MAS FRECUENTES ==========\n")

tipos_hurto = df_limpio.groupBy("tipo_de_hurto") \
    .agg(F.sum("cantidad").alias("total_hurtos")) \
    .orderBy(F.desc("total_hurtos"))

tipos_hurto.show(20, truncate=False)

# =========================
# ANALISIS 4
# Hurtos por genero
# =========================

print("\n========== DISTRIBUCION DE HURTOS POR GENERO ==========\n")

hurtos_genero = df_limpio.groupBy("genero") \
    .agg(F.sum("cantidad").alias("total_hurtos")) \
    .orderBy(F.desc("total_hurtos"))

hurtos_genero.show(truncate=False)

# =========================
# ANALISIS 5
# Hurtos por grupo etario
# =========================

print("\n========== DISTRIBUCION DE HURTOS POR GRUPO ETARIO ==========\n")

hurtos_grupo_etario = df_limpio.groupBy("grupo_etario") \
    .agg(F.sum("cantidad").alias("total_hurtos")) \
    .orderBy(F.desc("total_hurtos"))

hurtos_grupo_etario.show(truncate=False)

# =========================
# ANALISIS 6
# Medios o armas utilizados
# =========================

print("\n========== MEDIOS O ARMAS MAS REPORTADOS ==========\n")

armas = df_limpio.groupBy("armas_medios") \
    .agg(F.sum("cantidad").alias("total_hurtos")) \
    .orderBy(F.desc("total_hurtos"))

armas.show(20, truncate=False)

# =========================
# ANALISIS 7
# Evolucion anual
# =========================

print("\n========== EVOLUCION DE HURTOS POR AÑO ==========\n")

hurtos_anio = df_limpio.filter(F.col("anio").isNotNull()) \
    .groupBy("anio") \
    .agg(F.sum("cantidad").alias("total_hurtos")) \
    .orderBy("anio")

hurtos_anio.show(50, truncate=False)

# =========================
# ANALISIS 8
# Atlantico
# =========================

print("\n========== MUNICIPIOS DEL ATLANTICO CON MAYOR CANTIDAD DE HURTOS ==========\n")

hurtos_atlantico = df_limpio.filter(
    F.upper(F.col("departamento")) == "ATLÁNTICO"
).groupBy("municipio") \
 .agg(F.sum("cantidad").alias("total_hurtos")) \
 .orderBy(F.desc("total_hurtos"))

hurtos_atlantico.show(20, truncate=False)

print("\nAnalisis finalizado correctamente.")

input("\nPresione ENTER para finalizar y cerrar Spark...")

spark.stop()