# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Capítulo 5: procesamiento incremental
# MAGIC
# MAGIC **Dominios del examen:** Development & Ingestion (Auto Loader, COPY INTO) · Data Processing (Structured Streaming, MERGE).
# MAGIC **Tiempo:** 3 h. **Lee antes:** cap. 5 (Structured Streaming, checkpoints, triggers, Auto Loader, COPY INTO, evolución de esquema).
# MAGIC
# MAGIC Los logs ya vienen **un archivo por día**: es el caso perfecto para simular que los datos llegan día a día.
# MAGIC Una función copia N días desde el export a una carpeta de **llegadas**; cada vez que la corres, "llegan" días nuevos.
# MAGIC
# MAGIC En Free Edition el cómputo es serverless: usa `trigger(availableNow=True)` (procesa lo pendiente y se detiene). Los triggers continuos no están disponibles, pero sí entran en el examen.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

ORIGEN = "CM_PRE.ACTION_AUDIT"
DESTINO_LLEGADAS = f"{LLEGADAS}/action_audit_pre"
CK = f"{CHECKPOINTS}/action_audit_pre"
SCHEMA_LOC = f"{CHECKPOINTS}/action_audit_pre_schema"

def llegan(n_dias):
    """Copia los n_dias siguientes (del más antiguo al más nuevo) que aún no llegaron."""
    try:
        ya = {f.name for f in dbutils.fs.ls(DESTINO_LLEGADAS)}
    except Exception:
        ya = set()
    pendientes = [p for p in archivos_parquet(ORIGEN) if p.rsplit("/", 1)[-1] not in ya]
    for p in pendientes[:n_dias]:
        dbutils.fs.cp(p, f"{DESTINO_LLEGADAS}/{p.rsplit('/', 1)[-1]}")
    print(f"llegaron {min(n_dias, len(pendientes))} días; quedan {max(len(pendientes) - n_dias, 0)}")

def reiniciar_llegadas():
    """Borra llegadas, checkpoints y tablas del ejercicio para empezar de cero."""
    for p in [DESTINO_LLEGADAS, CK, SCHEMA_LOC, f"{CHECKPOINTS}/audit_pre_silver"]:
        dbutils.fs.rm(p, True)
    for t in ["bronze.stream_audit_pre", "bronze.copy_audit_pre", "silver.audit_pre"]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E5.1 · Auto Loader con `availableNow`
# MAGIC **Tareas:**
# MAGIC 1. `reiniciar_llegadas()` y `llegan(5)`.
# MAGIC 2. Completa el stream: Auto Loader (`cloudFiles`, formato parquet, `schemaLocation`) → tabla `bronze.stream_audit_pre`, con `_archivo` y `_ingesta`.
# MAGIC 3. Cuenta filas y archivos distintos (`_archivo`).
# MAGIC 4. `llegan(3)`, vuelve a correr el stream **sin cambiar nada** y cuenta de nuevo.
# MAGIC
# MAGIC **Terminado cuando:** la segunda corrida agregó **solo** las filas de los 3 días nuevos (ni una repetida).

# COMMAND ----------

reiniciar_llegadas()
llegan(5)

# COMMAND ----------

consulta = (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", SCHEMA_LOC)
            .load(DESTINO_LLEGADAS)
            .withColumn("_archivo", F.col("_metadata.file_path"))
            .withColumn("_ingesta", F.current_timestamp()))

# TODO: consulta.writeStream con checkpointLocation=CK, trigger(availableNow=True) y .toTable("bronze.stream_audit_pre")
#       luego q.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS filas, count(DISTINCT _archivo) AS dias FROM bronze.stream_audit_pre

# COMMAND ----------

# MAGIC %md
# MAGIC ## E5.2 · Qué guarda el checkpoint
# MAGIC **Tareas:**
# MAGIC 1. Lista `CK` con `dbutils.fs.ls`: ¿qué carpetas ves (`offsets`, `commits`, `sources`...)?
# MAGIC 2. Experimento: borra **solo** el checkpoint (`dbutils.fs.rm(CK, True)`), corre el stream otra vez y cuenta.
# MAGIC 3. Explica el resultado y vuelve a dejar la tabla bien (reiniciar y cargar de nuevo).
# MAGIC
# MAGIC **Terminado cuando:** puedes explicar por qué el checkpoint es lo que garantiza "exactamente una vez", y qué pasa si dos streams comparten el mismo checkpoint.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## E5.3 · COPY INTO, la alternativa por lotes
# MAGIC **Tareas:**
# MAGIC 1. Crea `bronze.copy_audit_pre` vacía y carga la carpeta de llegadas con `COPY INTO` (formato PARQUET).
# MAGIC 2. Córrelo dos veces: ¿la segunda carga algo?
# MAGIC 3. `llegan(2)` y córrelo otra vez.
# MAGIC
# MAGIC **Responde:** ¿cuándo elegirías COPY INTO y cuándo Auto Loader? (pista: cantidad de archivos y si quieres un stream continuo).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.copy_audit_pre;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: COPY INTO bronze.copy_audit_pre FROM '/Volumes/workspace/landing/raw/llegadas/action_audit_pre/'
# MAGIC --       FILEFORMAT = PARQUET COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC ## E5.4 · Stream de bronze a silver con tipos y sin duplicados
# MAGIC **Tareas:**
# MAGIC 1. Lee `bronze.stream_audit_pre` como **stream** (`spark.readStream.table`).
# MAGIC 2. Convierte la fecha (`F.expr(ts(COL['audit_fecha']))`) y quita duplicados por el ID del evento con `withWatermark` + `dropDuplicates`.
# MAGIC 3. Escribe `silver.audit_pre` con su propio checkpoint, `availableNow`.
# MAGIC 4. `llegan(2)`, corre bronze y luego silver: los dos avanzan solo con lo nuevo.
# MAGIC
# MAGIC **Terminado cuando:** `silver.audit_pre` no tiene IDs repetidos y avanza en cada llegada.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## E5.5 · MERGE incremental: estado actual de cada línea
# MAGIC CM_PRE.STATUS registra cada **cambio de estado** con su fecha (`DATE_CHANGE`). Con eso mantienes una tabla con el **último estado de cada línea**, actualizada día a día.
# MAGIC **Tareas:**
# MAGIC 1. `DESCRIBE bronze.cm_pre__status`: ¿cuál es la línea (SUB_ID o ISDN), el estado nuevo y la fecha?
# MAGIC 2. Crea `silver.estado_actual_pre` (línea, estado, fecha) vacía.
# MAGIC 3. Escribe un `foreachBatch` que, por lote: se quede con el último cambio por línea (`ROW_NUMBER`) y haga `MERGE`:
# MAGIC    `WHEN MATCHED AND origen.fecha > destino.fecha THEN UPDATE` · `WHEN NOT MATCHED THEN INSERT`.
# MAGIC 4. Hazlo llegar en dos tandas y verifica que una línea con dos cambios queda con el más reciente.
# MAGIC
# MAGIC **Terminado cuando:** correr el mismo lote dos veces no cambia nada (idempotente).

# COMMAND ----------

# TODO: usa como origen de llegadas los archivos de CM_PRE.STATUS (copia la idea de llegan())

# COMMAND ----------

# MAGIC %md
# MAGIC ## E5.6 · Evolución de esquema
# MAGIC Simula que un día el archivo trae una **columna nueva**:
# MAGIC 1. Toma un archivo de llegadas, agrega la columna `CANAL_NUEVO` con un valor fijo y guárdalo como un día nuevo en la carpeta de llegadas.
# MAGIC 2. Corre el stream de E5.1: falla. Lee el error.
# MAGIC 3. Córrelo de nuevo (con `cloudFiles.schemaEvolutionMode` por defecto, `addNewColumns`): ¿qué pasa ahora con la tabla?
# MAGIC 4. Repite con `rescue`: ¿dónde queda el dato nuevo?

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preguntas tipo examen
# MAGIC 1. ¿Qué diferencia hay entre `trigger(availableNow=True)`, `trigger(processingTime='5 minutes')` y el trigger por defecto?
# MAGIC 2. ¿Dónde guarda Auto Loader el esquema inferido y para qué sirve `schemaLocation`?
# MAGIC 3. Un stream se cayó a la mitad. Al reiniciarlo, ¿reprocesa todo, nada o desde dónde?
# MAGIC 4. ¿Qué hace `_rescued_data`?
# MAGIC 5. ¿COPY INTO vuelve a cargar un archivo que ya cargó si lo corres otra vez?
# MAGIC 6. ¿Por qué `dropDuplicates` en un stream necesita un watermark?
# MAGIC
# MAGIC <details><summary>Respuestas</summary>
# MAGIC
# MAGIC 1. `availableNow`: procesa lo pendiente en varios micro-lotes y se detiene. `processingTime`: un micro-lote cada intervalo, sin detenerse. Por defecto: un micro-lote apenas termina el anterior.
# MAGIC 2. En la ruta de `schemaLocation`; permite que el esquema persista entre corridas y que evolucione de forma controlada.
# MAGIC 3. Desde el último micro-lote confirmado en el checkpoint.
# MAGIC 4. Guarda en una columna JSON los datos que no calzan con el esquema (tipo distinto o columna nueva en modo `rescue`) en lugar de perderlos.
# MAGIC 5. No: es idempotente, recuerda qué archivos ya cargó.
# MAGIC 6. Sin watermark tendría que recordar todos los IDs para siempre (estado infinito); el watermark le dice cuándo puede olvidar.
# MAGIC </details>

