# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capítulo 3: entidades relacionales
# MAGIC
# MAGIC **Dominios del examen:** Development & Ingestion · Data Processing · Governance (comentarios, information_schema).
# MAGIC **Tiempo:** 2 h. **Lee antes:** cap. 3 (catálogos y esquemas, tablas administradas y externas, vistas, vistas temporales, CTAS, `INSERT OVERWRITE`, `INSERT INTO`, `MERGE`).
# MAGIC
# MAGIC Aquí cargas **todo** bronze con un solo bucle parametrizado y armas el primer diccionario de datos.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.1 · Cargar todo bronze con un bucle
# MAGIC **Tareas:**
# MAGIC 1. Completa la función `cargar_bronze(nombre)`:
# MAGIC    - tablas del núcleo: todos sus parquet;
# MAGIC    - logs: solo los `DIAS_LOGS` archivos más recientes (cada archivo es un día: `TABLA_YYYYMMDD.parquet`).
# MAGIC 2. Agrega `_archivo` y `_ingesta`.
# MAGIC 3. Escribe en modo `overwrite` con `overwriteSchema`.
# MAGIC 4. Corre el bucle y guarda un resumen (tabla, filas, segundos).
# MAGIC
# MAGIC **Terminado cuando:** todas las tablas presentes en el volumen tienen su tabla bronze y el resumen no tiene errores.

# COMMAND ----------

import time

def cargar_bronze(nombre):
    fs = archivos_parquet(nombre)
    if not fs:
        return None
    if nombre in LOGS and DIAS_LOGS:
        fs = fs[-DIAS_LOGS:]          # los nombres terminan en _YYYYMMDD: orden alfabético = orden de fechas
    df = (spark.read.parquet(*fs)
          .withColumn("_archivo", F.col("_metadata.file_path"))
          .withColumn("_ingesta", F.current_timestamp()))
    # TODO: escribe df en tb(nombre) con mode("overwrite") y option("overwriteSchema", "true")
    return spark.table(tb(nombre)).count()

resumen = []
for nombre in NUCLEO + LOGS:
    t0 = time.time()
    try:
        n = cargar_bronze(nombre)
        resumen.append((nombre, tb(nombre), n, round(time.time() - t0, 1), None if n is not None else "sin archivos"))
    except Exception as e:
        resumen.append((nombre, tb(nombre), None, round(time.time() - t0, 1), str(e)[:200]))
display(spark.createDataFrame(resumen, "origen string, tabla string, filas long, segundos double, error string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.2 · Administradas y externas
# MAGIC **Tarea:** corre `DESCRIBE EXTENDED` sobre una tabla bronze y anota `Type` y `Location`.
# MAGIC **Responde:** si haces `DROP TABLE` de una tabla administrada, ¿qué pasa con sus archivos? ¿Y en una externa? ¿Por qué en Free Edition no puedes crear externas? *(No hay external locations.)*

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bronze.cm_pos__sub_mb

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.3 · Vistas: persistentes y temporales
# MAGIC **Tareas:**
# MAGIC 1. Crea la vista **persistente** `silver.v_lineas_activas` con las líneas STATUS = '2' de prepago y pospago (`UNION ALL`, con una columna `tipo`).
# MAGIC 2. Crea una vista **temporal** `tmp_lineas_canceladas` (STATUS = '3').
# MAGIC 3. Abre otro notebook y prueba consultar las dos. ¿Cuál existe y cuál no?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE VIEW silver.v_lineas_activas AS ...

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE TEMP VIEW tmp_lineas_canceladas AS ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.4 · Decodificar estados con AP_DOMAIN
# MAGIC AP_DOMAIN es el catálogo de dominios: para cada `TYPE` (qué campo es), los códigos y su significado.
# MAGIC **Tareas:**
# MAGIC 1. Explora `bronze.cm_pre__ap_domain` y `bronze.cm_pos__ap_domain`: ¿qué `TYPE` corresponden a STATUS y ACT_STATUS de SUB_MB?
# MAGIC 2. Crea `silver.v_dominio` con (owner, type, codigo, significado) de los dos esquemas.
# MAGIC 3. Cuenta las líneas prepago por STATUS con su significado.
# MAGIC
# MAGIC **Terminado cuando:** cada STATUS de prepago tiene nombre. Si alguno no aparece en AP_DOMAIN, anótalo como hallazgo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.cm_pre__ap_domain ORDER BY 1 LIMIT 200

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE VIEW silver.v_dominio AS ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.5 · CTAS, INSERT OVERWRITE, INSERT INTO y MERGE
# MAGIC Con una tabla chica ves en `DESCRIBE HISTORY` qué operación deja cada comando.
# MAGIC **Tareas, en orden, sobre `gold.motivos`** (de `bronze.cm_pos__reason`):
# MAGIC 1. CTAS con los motivos.
# MAGIC 2. `INSERT OVERWRITE` con solo los primeros 10 motivos.
# MAGIC 3. `INSERT INTO` con los mismos 10 (quedan duplicados).
# MAGIC 4. `MERGE` que inserte los que faltan y no duplique.
# MAGIC 5. `DESCRIBE HISTORY gold.motivos`.
# MAGIC
# MAGIC **Responde:** ¿cuál de los cuatro es idempotente si lo corres dos veces? ¿Cuál cambia el esquema si la consulta trae una columna nueva?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: los 5 pasos

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.6 · Diccionario de datos con information_schema y comentarios
# MAGIC **Tareas:**
# MAGIC 1. Consulta `information_schema.columns` del esquema `bronze`: tabla, columna, tipo.
# MAGIC 2. Agrega comentarios a SUB_MB (prepago y pospago) con lo que **ya sabes** de las columnas:
# MAGIC    STATUS (2 activa, 3 cancelada, 1 registrada, 0 inactiva), ACT_STATUS (contiene 1 = bloqueo 1 vía, contiene 2 = bloqueo 2 vías),
# MAGIC    STA_DATETIME (alta; **se hereda en la migración**), END_DATETIME, MIG_DATETIME.
# MAGIC 3. Guarda el diccionario en `gold.diccionario_columnas` (incluye la columna `comment`).
# MAGIC
# MAGIC **Terminado cuando:** `gold.diccionario_columnas` tiene los comentarios de SUB_MB. Lo completas al final del cap. 4 con lo que descubras.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze.cm_pre__sub_mb ALTER COLUMN STATUS COMMENT '2 activa, 3 cancelada, 1 registrada, 0 inactiva (validar con AP_DOMAIN)';
# MAGIC -- TODO: el resto de comentarios

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE TABLE gold.diccionario_columnas AS SELECT ... FROM information_schema.columns WHERE table_schema = 'bronze'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preguntas tipo examen
# MAGIC 1. ¿Qué pasa con los datos al hacer `DROP TABLE` en una tabla administrada? ¿Y en una externa?
# MAGIC 2. Una vista temporal creada en un notebook, ¿se puede consultar desde un job distinto?
# MAGIC 3. ¿Qué comando reemplaza todos los datos de una tabla sin cambiar su esquema ni su historial, y falla si el esquema no calza?
# MAGIC 4. ¿Cuál es la jerarquía de objetos de Unity Catalog?
# MAGIC
# MAGIC <details><summary>Respuestas</summary>
# MAGIC
# MAGIC 1. Administrada: se borran metadatos y datos. Externa: solo metadatos; los archivos quedan.
# MAGIC 2. No: vive solo en la sesión que la creó.
# MAGIC 3. `INSERT OVERWRITE`.
# MAGIC 4. Metastore → catálogo → esquema → tablas, vistas, volúmenes, funciones y modelos.
# MAGIC </details>

