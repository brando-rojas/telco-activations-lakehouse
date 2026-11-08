# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Verificar los datos antes de empezar
# MAGIC
# MAGIC Dos chequeos, 15 minutos:
# MAGIC 1. **Inventario:** qué tablas llegaron al volumen, cuántos archivos y cuánto pesan, comparado con los `inventario_parte*.csv` de los zips.
# MAGIC 2. **Compuerta de datos personales:** muestrea cada tabla y busca valores con forma de celular, DNI, correo o nombre completo. Si los datos están seudonimizados, no debe aparecer nada (o solo falsos positivos que puedas explicar). **Si aparece algo real, no sigas** hasta seudonimizar esa columna.
# MAGIC
# MAGIC La compuerta es también práctica de examen: es un control de calidad que corre antes de que los datos entren al lakehouse (dominio *Data Governance & Quality*).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## E0b.1 · Inventario
# MAGIC **Tarea:** corre la celda. **Terminado cuando:** todas las tablas de `NUCLEO` dicen `OK`. Las que digan `FALTA` llegan en la parte 2: anótalas y sigue.

# COMMAND ----------

filas = []
for nombre in NUCLEO + LOGS:
    fs = []
    try:
        fs = [f for f in dbutils.fs.ls(ruta(nombre)) if f.name.endswith(".parquet")]
    except Exception:
        pass
    man = manifiesto(nombre)
    filas.append((nombre, "OK" if fs else "FALTA", len(fs), round(sum(f.size for f in fs) / 1024**3, 3),
                  man.get("exportadas"), "logs" if nombre in LOGS else "nucleo"))
inventario = spark.createDataFrame(filas, "tabla string, estado string, archivos int, gb double, filas_manifiesto long, grupo string")
display(inventario.orderBy("estado", "grupo", "tabla"))

# COMMAND ----------

# Los inventarios que vienen dentro de cada zip (qué parte trae qué días de cada log)
try:
    inv_zip = (spark.read.option("header", True).option("sep", ";")
               .csv(f"{RAW}/inventario_parte*.csv"))
    display(inv_zip)
except Exception as e:
    print("No encontré inventario_parte*.csv en", RAW, "-", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E0b.2 · Compuerta de datos personales
# MAGIC **Tarea:** corre la celda. Revisa cada hallazgo:
# MAGIC - `celular` en una columna ISDN, o `dni` en ID_NO / NUMBER → **dato real sin seudonimizar**. Para.
# MAGIC - `dni` en un ID interno de 8 dígitos (SUB_ID, SHOP_ID...) → falso positivo. Anótalo en `docs/decisiones.md` y sigue.
# MAGIC
# MAGIC **Terminado cuando:** la lista está vacía o todos los hallazgos están explicados como falsos positivos.

# COMMAND ----------

PATRONES = {
    "celular": r"^(\+?51)?9[0-9]{8}$",
    "dni":     r"^[0-9]{8}$",
    "correo":  r"^[^@ ]+@[^@ ]+\.[A-Za-z]{2,}$",
    "nombre":  r"^[A-ZÁÉÍÓÚÑ]{2,}( [A-ZÁÉÍÓÚÑ]{2,}){2,}$",   # 3+ palabras en mayúsculas
}
MUESTRA, UMBRAL = 20_000, 0.30

hallazgos = []
for nombre in NUCLEO + LOGS:
    fs = archivos_parquet(nombre)
    if not fs:
        continue
    df = spark.read.parquet(*fs[-3:]).limit(MUESTRA)          # los 3 archivos más recientes bastan
    cols = [c for c, t in df.dtypes if t == "string"]
    exprs = [F.avg(F.col(f"`{c}`").rlike(p).cast("int")).alias(f"{c}|{k}")
             for c in cols for k, p in PATRONES.items()]
    if not exprs:
        continue
    for clave, share in df.agg(*exprs).first().asDict().items():
        if share is not None and share >= UMBRAL:
            c, k = clave.split("|")
            hallazgos.append((nombre, c, k, round(float(share), 3)))

if hallazgos:
    display(spark.createDataFrame(hallazgos, "tabla string, columna string, patron string, proporcion double"))
else:
    print("Compuerta OK: ninguna columna con forma de dato personal en la muestra.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Para pensar (y para el examen)
# MAGIC 1. ¿Por qué el chequeo se hace sobre una **muestra** y no sobre toda la tabla? ¿Qué riesgo acepta?
# MAGIC 2. En Unity Catalog, ¿qué dos mecanismos te dejan mostrar una columna sensible solo a algunas personas sin duplicar la tabla? *(Lo practicas en el cap. 7: column masks y row filters.)*

