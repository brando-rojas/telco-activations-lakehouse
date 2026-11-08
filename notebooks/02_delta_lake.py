# Databricks notebook source
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
# MAGIC FROM read_files('/Volumes/workspace/landing/raw/EXPORT_PARQUET/CM_PRE.SUB_MB/', format => 'parquet');
# MAGIC
# MAGIC -- TODO: lo mismo para CM_POS.SUB_MB y CM_POS.MIGRATION_REQUESTS

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
# MAGIC CREATE OR REPLACE TABLE sandbox.sub_mb_pos SHALLOW CLONE bronze.cm_pos__sub_mb;
# MAGIC SELECT STATUS, count(*) AS n FROM sandbox.sub_mb_pos GROUP BY STATUS ORDER BY STATUS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El error: STATUS 2 = activa, 3 = cancelada
# MAGIC UPDATE sandbox.sub_mb_pos SET STATUS = '3' WHERE STATUS = '2';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: una consulta que muestre, por STATUS, el conteo actual y el de VERSION AS OF 0 lado a lado
# MAGIC -- Pista: dos subconsultas agrupadas y un FULL OUTER JOIN por STATUS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: RESTORE TABLE ... TO VERSION AS OF ...; luego vuelve a contar y mira DESCRIBE HISTORY

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
# MAGIC 1. Sí, con time travel o `RESTORE` (retención por defecto de archivos borrados: 7 días). Si un `VACUUM` ya borró los archivos de esas versiones, no.
# MAGIC 2. Crea una versión nueva; la historia anterior se mantiene.
# MAGIC 3. Particionar fija carpetas por columna (rígido, malo con alta cardinalidad); Z-ORDER reordena dentro de archivos al hacer OPTIMIZE; liquid clustering es incremental y sus claves se cambian con `ALTER TABLE ... CLUSTER BY` sin reescribir todo.
# MAGIC 4. Sí: `CREATE OR REPLACE` agrega una versión nueva al historial (a diferencia de `DROP` + `CREATE`).
# MAGIC </details>

