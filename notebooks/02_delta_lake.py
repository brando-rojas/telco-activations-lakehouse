# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 · Capítulo 2: Delta Lake
# MAGIC
# MAGIC **Dominios del examen:** Development & Ingestion · Data Processing.
# MAGIC **Tiempo:** 2 h. **Lee antes:** cap. 2 (transaction log, ACID, `DESCRIBE HISTORY/DETAIL`, time travel, `RESTORE`, `OPTIMIZE`, clustering, `VACUUM`, `CLONE`).
# MAGIC
# MAGIC Trabajas con las tablas de líneas: son las más grandes del núcleo (CM_PRE.SUB_MB ~58 M filas) y las que vas a reconstruir en el cap. 4.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## E2.1 · Primeras tablas bronze con CTAS
# MAGIC **Tarea:** crea `bronze.cm_pre__sub_mb`, `bronze.cm_pos__sub_mb` y `bronze.cm_pos__migration_requests` con `CREATE OR REPLACE TABLE ... AS SELECT` sobre `read_files`. Agrega dos columnas de linaje: `_archivo` (de `_metadata.file_path`) y `_ingesta` (`current_timestamp()`).
# MAGIC **Terminado cuando:** el conteo de cada tabla es igual a `exportadas` de su manifiesto (celda de verificación).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.cm_pre__sub_mb
# MAGIC COMMENT 'Lineas prepago (foto completa del export). Todo texto.'
# MAGIC AS SELECT *, _metadata.file_path AS _archivo, current_timestamp() AS _ingesta
# MAGIC FROM read_files('/Volumes/workspace/landing/raw/EXPORT_PARQUET/CM_PRE.SUB_MB/*.parquet', format => 'parquet');
# MAGIC
# MAGIC -- TODO: lo mismo para CM_POS.SUB_MB y CM_POS.MIGRATION_REQUESTS
# MAGIC CREATE OR REPLACE TABLE bronze.cm_pos__sub_mb
# MAGIC COMMENT 'Lineas pospago (foto completa del export). Todo texto.'
# MAGIC AS SELECT *, _metadata.file_path AS _archivo, current_timestamp() AS _ingesta
# MAGIC FROM read_files('/Volumes/workspace/landing/raw/EXPORT_PARQUET/CM_POS.SUB_MB/*.parquet', format => 'parquet');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bronze.cm_pos__migration_requests
# MAGIC COMMENT 'Solicitudes de CAMBIO DE PLAN pospago (plan_current -> plan_new). No es PRE->POS. Todo texto.'
# MAGIC AS SELECT *, _metadata.file_path AS _archivo, current_timestamp() AS _ingesta
# MAGIC FROM read_files('/Volumes/workspace/landing/raw/EXPORT_PARQUET/CM_POS.MIGRATION_REQUESTS/*.parquet', format => 'parquet');
# MAGIC

# COMMAND ----------

# Verificación contra el manifiesto del export
for nombre in ["CM_PRE.SUB_MB", "CM_POS.SUB_MB", "CM_POS.MIGRATION_REQUESTS"]:
    if not existe(tb(nombre)):
        print(f"{nombre}: falta crear {tb(nombre)}")
        continue
    n = spark.table(tb(nombre)).count()
    esperado = manifiesto(nombre).get("exportadas")
    if esperado is None:
        print(f"{nombre:<30} {n:>12,}  (sin manifiesto para comparar)")
    else:
        print(f"{nombre:<30} {n:>12,}  manifiesto {esperado:>12,}  {'OK' if n == esperado else 'DIFERENTE'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E2.2 · Lo que Delta guarda de cada tabla
# MAGIC **Tarea:** con `DESCRIBE DETAIL` anota formato, número de archivos y tamaño de `bronze.cm_pre__sub_mb`. Con `DESCRIBE HISTORY` mira qué operación quedó en la versión 0.
# MAGIC **Pregunta:** el Parquet de origen ocupa X GB (E1.1). ¿La tabla Delta ocupa lo mismo? ¿Por qué podría ser distinto?

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL bronze.cm_pre__sub_mb

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze.cm_pre__sub_mb

# COMMAND ----------

# MAGIC %md
# MAGIC ## E2.3 · Un error masivo y cómo volver atrás
# MAGIC Simulas el peor día: alguien corre un `UPDATE` sin el filtro correcto y "da de baja" a todas las líneas activas de pospago.
# MAGIC Para no tocar bronze, trabajas sobre un **clon superficial** (`SHALLOW CLONE`): copia solo metadatos, apunta a los mismos archivos.
# MAGIC
# MAGIC **Tareas:**
# MAGIC 1. Crea `sandbox.sub_mb_pos` como `SHALLOW CLONE` de `bronze.cm_pos__sub_mb`.
# MAGIC 2. Cuenta líneas por `STATUS` (versión 0).
# MAGIC 3. Corre el `UPDATE` erróneo.
# MAGIC 4. Compara la versión actual con `VERSION AS OF 0` usando una sola consulta.
# MAGIC 5. Recupera con `RESTORE` y verifica que los conteos vuelven a los de la versión 0.
# MAGIC 6. Mira `DESCRIBE HISTORY`: ¿el `RESTORE` borró la versión con el error?
# MAGIC
# MAGIC **Terminado cuando:** los conteos por STATUS después del RESTORE son idénticos a la versión 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sandbox.sub_mb_pos;
# MAGIC CREATE TABLE sandbox.sub_mb_pos SHALLOW CLONE bronze.cm_pos__sub_mb;
# MAGIC SELECT STATUS, count(*) AS n FROM sandbox.sub_mb_pos GROUP BY STATUS ORDER BY STATUS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El error: STATUS 2 = activa, 3 = cancelada
# MAGIC UPDATE sandbox.sub_mb_pos SET STATUS = '3' WHERE STATUS = '2';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: una consulta que muestre, por STATUS, el conteo actual y el de VERSION AS OF 0 lado a lado
# MAGIC -- Pista: dos subconsultas agrupadas y un FULL OUTER JOIN por STATUS
# MAGIC -- TODO 1 de E2.3 · versión actual vs versión 0, lado a lado
# MAGIC WITH actual AS (
# MAGIC   SELECT STATUS, count(*) AS n FROM sandbox.sub_mb_pos GROUP BY STATUS),
# MAGIC v0 AS (
# MAGIC   SELECT STATUS, count(*) AS n FROM sandbox.sub_mb_pos VERSION AS OF 0 GROUP BY STATUS)
# MAGIC SELECT coalesce(a.STATUS, v.STATUS)        AS STATUS,
# MAGIC        v.n                                 AS n_version_0,
# MAGIC        a.n                                 AS n_actual,
# MAGIC        coalesce(a.n, 0) - coalesce(v.n, 0) AS diferencia
# MAGIC FROM actual a
# MAGIC FULL OUTER JOIN v0 v ON a.STATUS <=> v.STATUS
# MAGIC ORDER BY STATUS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: RESTORE TABLE ... TO VERSION AS OF ...; luego vuelve a contar y mira DESCRIBE HISTORY
# MAGIC -- TODO 2 de E2.3 · volver atrás y comprobar
# MAGIC RESTORE TABLE sandbox.sub_mb_pos TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH actual AS (
# MAGIC   SELECT STATUS, count(*) AS n FROM sandbox.sub_mb_pos GROUP BY STATUS),
# MAGIC v0 AS (
# MAGIC   SELECT STATUS, count(*) AS n FROM sandbox.sub_mb_pos VERSION AS OF 0 GROUP BY STATUS)
# MAGIC SELECT coalesce(a.STATUS, v.STATUS)        AS STATUS,
# MAGIC        v.n                                 AS n_version_0,
# MAGIC        a.n                                 AS n_actual,
# MAGIC        coalesce(a.n, 0) - coalesce(v.n, 0) AS diferencia
# MAGIC FROM actual a
# MAGIC FULL OUTER JOIN v0 v ON a.STATUS <=> v.STATUS
# MAGIC ORDER BY STATUS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sandbox.sub_mb_pos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E2.4 · Clustering para buscar por número
# MAGIC Las consultas de trazabilidad casi siempre filtran por número (ISDN). **Tareas:**
# MAGIC 1. Anota `numFiles` de `DESCRIBE DETAIL bronze.cm_pre__sub_mb`.
# MAGIC 2. Busca un ISDN cualquiera (toma uno con `LIMIT 1`) y mira en el **Query Profile** cuántos archivos leyó.
# MAGIC 3. Activa *liquid clustering* por `ISDN`, corre `OPTIMIZE` y repite la búsqueda.
# MAGIC
# MAGIC **Terminado cuando:** puedes decir cuántos archivos se saltó la búsqueda después del clustering, y por qué `OPTIMIZE` corrido dos veces seguidas no cambia nada la segunda vez.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze.cm_pre__sub_mb CLUSTER BY (ISDN);
# MAGIC OPTIMIZE bronze.cm_pre__sub_mb;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: busca un ISDN antes y después; compara archivos leídos en el Query Profile
# MAGIC DESCRIBE HISTORY bronze.cm_pre__sub_mb; 
# MAGIC
# MAGIC DECLARE OR REPLACE VARIABLE isdn_prueba STRING;
# MAGIC SET VAR isdn_prueba = (SELECT ISDN FROM bronze.cm_pre__sub_mb WHERE ISDN IS NOT NULL LIMIT 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.cm_pre__sub_mb VERSION AS OF 0 WHERE ISDN = isdn_prueba;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.cm_pre__sub_mb WHERE ISDN = isdn_prueba;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL bronze.cm_pre__sub_mb;

# COMMAND ----------

# MAGIC %sql
# MAGIC --RESTORE TABLE bronze.cm_pre__sub_mb TO VERSION AS OF 0;
# MAGIC --DESCRIBE DETAIL bronze.cm_pre__sub_mb;   -- numFiles debe volver a 27; mira clusteringColumns

# COMMAND ----------

# MAGIC %md
# MAGIC ## E2.5 · VACUUM y el límite del time travel
# MAGIC **Tareas:**
# MAGIC 1. Corre `VACUUM sandbox.sub_mb_pos DRY RUN` y anota cuántos archivos borraría.
# MAGIC 2. Responde sin correrlo: si corrieras `VACUUM ... RETAIN 0 HOURS` (con el chequeo de seguridad desactivado), ¿podrías seguir haciendo `VERSION AS OF 0`?
# MAGIC 3. ¿Por qué el `VACUUM` de un *shallow clone* no puede borrar los archivos de la tabla original?

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM sandbox.sub_mb_pos DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **¿Cuántos archivos borraría?** Lo más probable es que 0: los archivos que dejó de usar el clon (los del UPDATE, que el RESTORE sacó) tienen menos de 7 días, y VACUUM solo borra archivos sin referencia más viejos que la retención.
# MAGIC 2. **Con `RETAIN 0 HOURS`, ¿seguirías pudiendo hacer `VERSION AS OF 0`?** En general, no: VACUUM borra todo archivo que no use la versión actual, y las versiones viejas que dependían de ellos ya no se pueden leer ni restaurar. En este caso hay un detalle: después del RESTORE, la versión actual usa exactamente los archivos de la versión 0, así que la 0 seguiría legible. La que se rompería es la versión 1 (la del error). Para probarlo habría que desactivar spark.databricks.delta.retentionDurationCheck.enabled, y serverless solo deja cambiar unas pocas propiedades de Spark: es probable que no puedas. Respóndelo en teoría.
# MAGIC 3. **¿Puede el VACUUM del clon borrar archivos de la tabla original?** La pregunta del notebook asume que no. En tablas administradas de Unity Catalog, la documentación dice que el VACUUM de la fuente o del clon puede borrar archivos de la fuente, pero solo los que ya no son válidos para ninguna de las dos: Unity Catalog considera válido todo archivo dentro de la retención de la fuente o de cualquier clon. En tablas externas, el VACUUM solo borra archivos de la fuente cuando se corre sobre la fuente. Lo que sí se mantiene: VACUUM sobre la fuente no rompe un clon que todavía usa sus archivos.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E2.6 · Tu primera decisión documentada
# MAGIC Escribe en `docs/decisiones.md` (en el repo):
# MAGIC - por qué bronze guarda todo como texto;
# MAGIC - por qué usas `SHALLOW CLONE` para experimentar y cuándo usarías `DEEP CLONE`;
# MAGIC - qué columna elegiste para clustering y por qué.
# MAGIC
# MAGIC ## Preguntas tipo examen
# MAGIC 1. Un analista borró filas por error hace 3 días. La retención es la de fábrica. ¿Puede recuperarlas? ¿Y si alguien corrió `VACUUM` con retención de 1 hora ayer?
# MAGIC 2. ¿`RESTORE` crea una versión nueva o reescribe la historia?
# MAGIC 3. ¿Qué diferencia hay entre particionar, Z-ORDER y liquid clustering? ¿Cuál se puede cambiar sin reescribir la tabla?
# MAGIC 4. ¿`CREATE OR REPLACE TABLE ... AS SELECT` conserva el historial de la tabla anterior?
# MAGIC
# MAGIC <details><summary>Respuestas</summary>
# MAGIC
# MAGIC 1. Sí, con time travel o `RESTORE` (retención por defecto de archivos borrados: 7 días). Si un `VACUUM` ya borró los archivos de esas versiones, no. `UNDROP TABLE` recupera una tabla administrada borrada (no filas) durante unos días; es otra herramienta de Unity Catalog.
# MAGIC 2. Crea una versión nueva; la historia anterior se mantiene.
# MAGIC 3. Particionar fija carpetas por columna (rígido, malo con alta cardinalidad); Z-ORDER reordena dentro de archivos al hacer OPTIMIZE; liquid clustering es incremental y sus claves se cambian con `ALTER TABLE ... CLUSTER BY` sin reescribir todo.
# MAGIC 4. Sí: `CREATE OR REPLACE` agrega una versión nueva al historial (a diferencia de `DROP` + `CREATE`).
# MAGIC </details>
# MAGIC