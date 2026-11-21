# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
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
# TODO de E5.1 · debajo de "consulta = (...)"
q = (consulta.writeStream
     .option("checkpointLocation", CK)
     .option("mergeSchema", "true")          # para E5.6: deja entrar columnas nuevas a la tabla Delta
     .trigger(availableNow=True)
     .toTable("bronze.stream_audit_pre"))
q.awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS filas, count(DISTINCT _archivo) AS dias FROM bronze.stream_audit_pre

# COMMAND ----------

# Opcional pero útil: el mismo stream en una función, para repetirlo en E5.2 y E5.6 sin copiar celdas
def correr_stream_bronze(modo_evolucion=None):
    lector = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "parquet")
              .option("cloudFiles.schemaLocation", SCHEMA_LOC))
    if modo_evolucion:
        lector = lector.option("cloudFiles.schemaEvolutionMode", modo_evolucion)
    q = (lector.load(DESTINO_LLEGADAS)
         .withColumn("_archivo", F.col("_metadata.file_path"))
         .withColumn("_ingesta", F.current_timestamp())
         .writeStream
         .option("checkpointLocation", CK)
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .toTable("bronze.stream_audit_pre"))
    q.awaitTermination()
 
# paso 4 del ejercicio
llegan(3)
correr_stream_bronze()
display(spark.sql("SELECT count(*) AS filas, count(DISTINCT _archivo) AS dias FROM bronze.stream_audit_pre"))


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
# 1) qué guarda el checkpoint
for f in dbutils.fs.ls(CK):
    print(f.name)                                  # commits/  metadata  offsets/  sources/
print(dbutils.fs.head(f"{CK}/offsets/0", 3000))    # primer micro-lote: hasta dónde leyó


# COMMAND ----------

# 2) experimento: borrar SOLO el checkpoint
antes = spark.table("bronze.stream_audit_pre").count()
dbutils.fs.rm(CK, True)
correr_stream_bronze()
despues = spark.table("bronze.stream_audit_pre").count()
print(f"antes {antes:,} · después {despues:,} · agregó {despues - antes:,}")
display(spark.sql("""SELECT _archivo, count(*) AS filas, count(DISTINCT _ingesta) AS cargas
                     FROM bronze.stream_audit_pre GROUP BY _archivo ORDER BY _archivo"""))

# COMMAND ----------

# 3) dejarlo bien: todo de cero y los mismos 8 días (5 + 3)
reiniciar_llegadas()
llegan(8)
correr_stream_bronze()

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
# MAGIC COPY INTO bronze.copy_audit_pre
# MAGIC FROM (SELECT *, _metadata.file_path AS _archivo
# MAGIC       FROM '/Volumes/workspace/landing/raw/llegadas/action_audit_pre/')
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC -- resultado: num_affected_rows / num_inserted_rows. Córrelo otra vez: 0 filas.
# MAGIC -- luego, en Python: llegan(2)  y vuelve a correr esta celda: entran solo los 2 archivos nuevos
# MAGIC  
# MAGIC SELECT _archivo, count(*) AS filas FROM bronze.copy_audit_pre GROUP BY _archivo ORDER BY _archivo;

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
CK_SILVER = f"{CHECKPOINTS}/audit_pre_silver"      # la misma ruta que borra reiniciar_llegadas()
ID, FECHA = COL['audit_id'], COL['audit_fecha']
 
def correr_stream_silver():
    q = (spark.readStream.table("bronze.stream_audit_pre")
         .withColumn("issue_ts", F.expr(ts(FECHA)))
         .where("issue_ts IS NOT NULL")                 # sin fecha no hay watermark: esas filas van a calidad (cap. 6)
         .withWatermark("issue_ts", "2 days")
         .dropDuplicates([ID, "issue_ts"])              # la columna del watermark va en la clave
         .writeStream
         .option("checkpointLocation", CK_SILVER)
         .trigger(availableNow=True)
         .toTable("silver.audit_pre"))
    q.awaitTermination()
 
correr_stream_silver()
display(spark.sql(f"""SELECT count(*) AS filas, count(DISTINCT {ID}) AS ids,
                             count(*) - count(DISTINCT {ID}) AS repetidos FROM silver.audit_pre"""))
 
# paso 4: llegan 2 días, avanza bronze y luego silver
llegan(2)
correr_stream_bronze()
correr_stream_silver()
display(spark.sql(f"SELECT count(*) AS filas, count(DISTINCT {ID}) AS ids FROM silver.audit_pre"))


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
# 1) DESCRIBE primero y ajusta estos tres nombres
display(spark.sql(f"DESCRIBE {tb('CM_PRE.STATUS')}"))


# COMMAND ----------

C_LINEA, C_ESTADO, C_ANTERIOR, C_FECHA = "ISDN", "NEW_STATUS", "OLD_STATUS", "DATE_CHANGE"

ORIGEN_ST = "CM_PRE.STATUS"
LLEG_ST   = f"{LLEGADAS}/status_pre"               # la misma carpeta que lee el pipeline del cap. 6 (E6.4)
CK_ST     = f"{CHECKPOINTS}/status_pre_merge"
SCHEMA_ST = f"{CHECKPOINTS}/status_pre_schema"

def llegan_status(n_dias):
    try:
        ya = {f.name for f in dbutils.fs.ls(LLEG_ST)}
    except Exception:
        ya = set()
    pendientes = [p for p in archivos_parquet(ORIGEN_ST) if p.rsplit("/", 1)[-1] not in ya]
    for p in pendientes[:n_dias]:
        dbutils.fs.cp(p, f"{LLEG_ST}/{p.rsplit('/', 1)[-1]}")
    print(f"llegaron {min(n_dias, len(pendientes))} días de STATUS; quedan {max(len(pendientes) - n_dias, 0)}")

def reiniciar_status():
    """Borra llegadas, checkpoint, esquema y tabla de E5.5, y crea el destino vacío."""
    for p in [LLEG_ST, CK_ST, SCHEMA_ST]:
        dbutils.fs.rm(p, True)
    spark.sql("DROP TABLE IF EXISTS silver.estado_actual_pre")
    spark.sql("""CREATE TABLE silver.estado_actual_pre
                 (isdn9 STRING, estado STRING, estado_anterior STRING, fecha_ts TIMESTAMP, _actualizado TIMESTAMP)
                 COMMENT 'Ultimo cambio de estado de cada numero prepago (CM_PRE.STATUS), mantenido con MERGE incremental'""")

# por lote: último cambio por número y MERGE
def merge_lote(lote, batch_id):
    w = Window.partitionBy("isdn9").orderBy(F.col("fecha_ts").desc(), F.col("estado").desc())
    (lote.select(F.expr(f"nullif({isdn9(C_LINEA)}, '')").alias("isdn9"),
                 F.col(C_ESTADO).alias("estado"),
                 F.col(C_ANTERIOR).alias("estado_anterior"),
                 F.expr(ts(f"left({C_FECHA}, 19)")).alias("fecha_ts"))     # left 19: ignora fracciones de segundo
         .where("isdn9 IS NOT NULL AND fecha_ts IS NOT NULL")
         .withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
         .createOrReplaceTempView("lote_estado"))
    lote.sparkSession.sql("""
        MERGE INTO silver.estado_actual_pre d
        USING lote_estado o
        ON d.isdn9 = o.isdn9
        WHEN MATCHED AND o.fecha_ts > d.fecha_ts THEN
          UPDATE SET d.estado = o.estado, d.estado_anterior = o.estado_anterior,
                     d.fecha_ts = o.fecha_ts, d._actualizado = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT (isdn9, estado, estado_anterior, fecha_ts, _actualizado)
          VALUES (o.isdn9, o.estado, o.estado_anterior, o.fecha_ts, current_timestamp())""")

def correr_merge_status():
    q = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "parquet")
         .option("cloudFiles.schemaLocation", SCHEMA_ST)
         .load(LLEG_ST)
         .writeStream
         .foreachBatch(merge_lote)
         .option("checkpointLocation", CK_ST)
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()

# de cero: la corrida que falló dejó la tabla con sub_id y el checkpoint a medias
reiniciar_status()
llegan_status(3)

# COMMAND ----------

# antes del stream: ¿convierten el número y la fecha? (si con_fecha es 0, mira el formato de DATE_CHANGE)
display(spark.read.parquet(LLEG_ST).selectExpr(
    "count(*) AS filas",
    f"count(nullif({isdn9(C_LINEA)}, '')) AS con_numero",
    f"count({ts(f'left({C_FECHA}, 19)')}) AS con_fecha",
    f"min({C_FECHA}) AS ejemplo_fecha"))

# COMMAND ----------

# dos tandas
correr_merge_status()
llegan_status(3); correr_merge_status()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar (reemplaza la celda de verificación): ¿cada número con 2+ cambios quedó con el más reciente?
# MAGIC WITH todos AS (
# MAGIC SELECT nullif(silver.f_isdn9(ISDN), '') AS isdn9, silver.f_ts(DATE_CHANGE) AS f
# MAGIC    FROM read_files('/Volumes/workspace/landing/raw/llegadas/status_pre/', format => 'parquet')),
# MAGIC ult AS (
# MAGIC    SELECT isdn9, max(f) AS fecha, count(*) AS cambios
# MAGIC    FROM todos WHERE f IS NOT NULL AND isdn9 IS NOT NULL GROUP BY isdn9)
# MAGIC SELECT count(*) AS numeros_con_varios_cambios,
# MAGIC     count_if(e.fecha_ts = u.fecha) AS quedaron_con_el_ultimo
# MAGIC FROM ult u JOIN silver.estado_actual_pre e ON e.isdn9 = u.isdn9
# MAGIC WHERE u.cambios > 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC -- idempotencia: mira las métricas del último MERGE (numTargetRowsUpdated / numTargetRowsInserted)
# MAGIC DESCRIBE HISTORY silver.estado_actual_pre;

# COMMAND ----------

# prueba de idempotencia: el mismo lote dos veces, a mano (esta celda no cambia)
un_dia = spark.read.parquet(sorted(f.path for f in dbutils.fs.ls(LLEG_ST))[0])
merge_lote(un_dia, 900)

# COMMAND ----------

merge_lote(un_dia, 901)       # en History, este MERGE debe tener 0 filas actualizadas e insertadas

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
# 1) un "día nuevo" con una columna más
def crear_dia_con_columna_nueva():
    base = sorted(f.path for f in dbutils.fs.ls(DESTINO_LLEGADAS) if f.name.endswith(".parquet"))[-1]
    tmp = f"{LLEGADAS}/_tmp_canal"
    (spark.read.parquet(base).limit(1000)
          .withColumn("CANAL_NUEVO", F.lit("APP"))
          .coalesce(1).write.mode("overwrite").parquet(tmp))
    parte = [f.path for f in dbutils.fs.ls(tmp) if f.name.endswith(".parquet")][0]
    dbutils.fs.cp(parte, f"{DESTINO_LLEGADAS}/zz_canal_nuevo_99991231.parquet")
    dbutils.fs.rm(tmp, True)
 
crear_dia_con_columna_nueva()

# COMMAND ----------

# 2) primera corrida: falla (addNewColumns por defecto)
try:
    correr_stream_bronze()
except Exception as e:
    print(type(e).__name__, str(e)[:600])

# COMMAND ----------

# 3) segunda corrida, sin cambiar nada: sigue con el esquema nuevo
correr_stream_bronze()
display(spark.sql("""SELECT CANAL_NUEVO, count(*) FROM bronze.stream_audit_pre GROUP BY CANAL_NUEVO"""))

# COMMAND ----------

# 4) lo mismo en modo rescue (de cero, para que el esquema guardado no tenga aún la columna)
reiniciar_llegadas(); llegan(5)
correr_stream_bronze(modo_evolucion="rescue")
crear_dia_con_columna_nueva()
correr_stream_bronze(modo_evolucion="rescue")       # no falla
display(spark.sql("""SELECT _rescued_data, _rescued_data:CANAL_NUEVO AS canal
                     FROM bronze.stream_audit_pre WHERE _rescued_data IS NOT NULL LIMIT 5"""))


# COMMAND ----------

dbutils.fs.rm(f"{DESTINO_LLEGADAS}/zz_canal_nuevo_99991231.parquet")

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
# MAGIC