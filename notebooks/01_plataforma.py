# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Capítulo 1: la plataforma
# MAGIC
# MAGIC **Dominio del examen:** Databricks Intelligence Platform.
# MAGIC **Tiempo:** 1 h. **Lee antes:** cap. 1 (lakehouse, plano de control y de cómputo, tipos de cómputo, notebooks, Git folders).
# MAGIC
# MAGIC Al terminar deberías poder explicar sin mirar:
# MAGIC - qué vive en el plano de control y qué en el de cómputo, y dónde corre el cómputo serverless;
# MAGIC - cuándo usar cómputo *all-purpose*, *job compute* o un *SQL warehouse*;
# MAGIC - qué es un volumen de Unity Catalog y por qué reemplaza las rutas `dbfs:/`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## E1.1 · Explorar el volumen con `dbutils.fs` y con SQL
# MAGIC **Tarea:** lista `RAW` de las dos formas. Después cuenta cuántas carpetas (tablas) hay y cuánto pesan en total.
# MAGIC **Terminado cuando:** tu total en GB coincide (±1 %) con lo que dijo `empaquetar.py` para las partes que subiste.

# COMMAND ----------

carpetas = dbutils.fs.ls(RAW)
display(carpetas)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lo mismo con SQL (cambia la ruta si tu catálogo no es workspace)
# MAGIC LIST '/Volumes/workspace/landing/raw/EXPORT_PARQUET/'

# COMMAND ----------

# TODO: recorre cada carpeta, suma f.size de sus .parquet y muestra un DataFrame
#       (tabla, archivos, gb) ordenado por gb descendente, y el total.
# Pista: dbutils.fs.ls devuelve FileInfo(path, name, size, modificationTime)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E1.2 · Consultar archivos sin crear tablas
# MAGIC Hay dos formas de leer Parquet directamente: `parquet.\`ruta\`` y `read_files(...)`.
# MAGIC **Tarea:** lee 10 filas de `CM_POS.SUB_MB` con cada una y compara: ¿qué columnas extra puedes pedir con `read_files`? (prueba `_metadata.file_path`).
# MAGIC **Terminado cuando:** puedes decir qué archivo trae cada fila.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`/Volumes/workspace/landing/raw/EXPORT_PARQUET/CM_POS.SUB_MB/` LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_path AS archivo, *
# MAGIC FROM read_files('/Volumes/workspace/landing/raw/EXPORT_PARQUET/CM_POS.SUB_MB/', format => 'parquet')
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## E1.3 · Widgets y comandos mágicos
# MAGIC **Tarea:** crea un widget desplegable con las tablas de `NUCLEO`. Al elegir una, muestra su esquema y cuántas filas tiene.
# MAGIC Usa al menos una vez: `%md`, `%sql`, `%run`, `%fs` y `%pip` (para `%pip`, instala algo pequeño, por ejemplo `tabulate`).
# MAGIC **Terminado cuando:** cambiar el widget cambia el resultado sin editar código.

# COMMAND ----------

dbutils.widgets.dropdown("tabla", "CM_PRE.SUB_MB", NUCLEO, "Tabla")

# COMMAND ----------

# TODO: lee dbutils.widgets.get("tabla"), carga sus parquet con spark.read.parquet(...)
#       y muestra df.printSchema() y df.count().

# COMMAND ----------

# MAGIC %md
# MAGIC ## E1.4 · Cómputo
# MAGIC **Tarea:** corre la misma consulta (conteo de `CM_PRE.SUB_MB` por `STATUS`) en este notebook (serverless) y en el **SQL editor** con un SQL warehouse. Abre el **Query Profile** de la segunda.
# MAGIC Anota en `docs/decisiones.md`: cuánto tardó cada una, cuántos archivos leyó y qué tipo de cómputo usarías para un dashboard y cuál para un job nocturno.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT STATUS, count(*) AS lineas
# MAGIC FROM parquet.`/Volumes/workspace/landing/raw/EXPORT_PARQUET/CM_PRE.SUB_MB/`
# MAGIC GROUP BY STATUS ORDER BY lineas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E1.5 · Git folder
# MAGIC **Tarea:** pon esta carpeta de notebooks en un Git folder conectado a tu repo `telco-activations-lakehouse`. Crea la rama `feat/telco-ejercicios`, haz commit desde Databricks y el PR en GitHub.
# MAGIC **Terminado cuando:** el PR está fusionado en GitHub y haces *pull* desde Databricks.
# MAGIC Recuerda: el **merge se hace en GitHub**, no en Databricks. Los datos nunca van al repo (solo notebooks y `docs/`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preguntas tipo examen
# MAGIC 1. Un equipo quiere que su cómputo no corra en su propia cuenta de nube. ¿Qué tipo de cómputo elige?
# MAGIC 2. ¿Qué diferencia hay entre `parquet.\`ruta\`` y `read_files`? ¿Alguno de los dos es incremental?
# MAGIC 3. ¿Dónde guardarías archivos de llegada (CSV, Parquet) gobernados por Unity Catalog si no puedes usar `dbfs:/`?
# MAGIC 4. Un notebook hace `%run ./config`. ¿Las variables de Python definidas en `config` quedan disponibles después? ¿Y con `dbutils.notebook.run`?
# MAGIC
# MAGIC <details><summary>Respuestas</summary>
# MAGIC
# MAGIC 1. Serverless: el cómputo corre en la cuenta de Databricks.
# MAGIC 2. Los dos leen archivos directo; `read_files` acepta opciones de formato y expone `_metadata`. Ninguno es incremental: cada consulta relee todo (lo incremental es Auto Loader / streaming tables, cap. 5).
# MAGIC 3. En un volumen de Unity Catalog (`/Volumes/catalogo/esquema/volumen/...`).
# MAGIC 4. Con `%run` sí (corre en el mismo contexto). `dbutils.notebook.run` corre otro notebook aparte y solo devuelve un texto con `dbutils.notebook.exit`.
# MAGIC </details>

