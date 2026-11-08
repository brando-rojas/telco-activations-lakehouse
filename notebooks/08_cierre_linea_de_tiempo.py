# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Cierre: la línea de tiempo de cada número
# MAGIC
# MAGIC **Junta todo lo de los capítulos 2 a 7.** Tiempo: 3–4 h.
# MAGIC
# MAGIC El objetivo de todo el ejercicio: una tabla `gold.linea_tiempo` con **una fila por evento** de cada número, con su fecha real y de qué tabla sale.
# MAGIC Con ella respondes "¿qué le pasó a este número y cuándo?" sin abrir ACTION_AUDIT línea por línea.
# MAGIC
# MAGIC | tipo_evento | Fuente | Fecha |
# MAGIC |---|---|---|
# MAGIC | ALTA | `silver.lineas` | `inicio_ts` (tu regla del cap. 4, no el STA heredado) |
# MAGIC | BAJA | `silver.lineas` | `baja_ts` |
# MAGIC | MIGRACION | `silver.transiciones` (PRE→POS / POS→PRE, mismo cliente) | inicio del periodo nuevo |
# MAGIC | RECICLAJE | `silver.transiciones` (cambio de cliente) | inicio del periodo nuevo |
# MAGIC | CAMBIO_TITULAR | `bronze.cm_pos__change_owner_log` | fecha del cambio |
# MAGIC | PORTABILIDAD_IN / _OUT | `bronze.mnp__porting_request` (TYPE '01' entrada, '05' salida; STATE '00A02' ejecutada) | fecha de ejecución |
# MAGIC | SOLICITUD_BAJA | `bronze.cm_pos__terminate_req` / `bronze.cm_pre__termination_sub` | fecha de la solicitud |
# MAGIC | CAMBIO_SIM | `bronze.cm_pos__req_change_sim` / `bronze.cm_pre__sub_sim_transfer` | fecha del cambio |
# MAGIC | CAMBIO_NUMERO | `bronze.cm_pos__sub_change_isdn` | fecha del cambio |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## E8.1 · Construir `gold.linea_tiempo`
# MAGIC **Tareas:**
# MAGIC 1. Una CTE por fila de la tabla de arriba, todas con las mismas columnas:
# MAGIC    `isdn9, sub_id, tipo_linea, tipo_evento, fecha_ts, fuente, detalle`.
# MAGIC 2. `UNION ALL` de todas.
# MAGIC 3. Clustering por `isdn9`.
# MAGIC
# MAGIC **Terminado cuando:** cada fuente aporta filas (cuenta por `fuente`) y ninguna tiene `fecha_ts` nula.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE TABLE gold.linea_tiempo CLUSTER BY (isdn9) AS
# MAGIC -- WITH altas AS (...), bajas AS (...), migraciones AS (...), ...
# MAGIC -- SELECT * FROM altas UNION ALL SELECT * FROM bajas UNION ALL ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## E8.2 · Consistencia entre fuentes
# MAGIC Con la línea de tiempo, busca contradicciones. Cada una es un hallazgo para el diccionario:
# MAGIC 1. BAJA en SUB_MB **sin** solicitud de baja ni portabilidad de salida en ±3 días: ¿bajas automáticas? (cruza con `auto_terminate_sub` / `auto_cancel_sub`)
# MAGIC 2. PORTABILIDAD_OUT ejecutada **sin** BAJA en SUB_MB después.
# MAGIC 3. CAMBIO_TITULAR **después** de la BAJA.
# MAGIC 4. Eventos con fecha **anterior** al ALTA del número.
# MAGIC
# MAGIC **Terminado cuando:** tienes una tabla `gold.inconsistencias` (regla, isdn9, detalle) con un conteo por regla.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## E8.3 · Diccionario final
# MAGIC Completa `gold.diccionario_columnas` (caps. 3 y 4) para SUB_MB y los trámites con:
# MAGIC - qué significa cada fecha **según los datos** (no según el nombre);
# MAGIC - qué tan confiable es (con el % que lo respalda);
# MAGIC - qué fuente usar para cada pregunta.
# MAGIC
# MAGIC Después escríbelo como documento legible (`docs/diccionario_trazabilidad.md`): significado de cada columna, ejemplos, valores cerrados con su significado y relaciones entre tablas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E8.4 · Preguntas que ahora puedes responder
# MAGIC Escribe la consulta y el resultado de cada una en `docs/hallazgos.md`:
# MAGIC 1. ¿Cuántas migraciones PRE→POS y POS→PRE hubo por mes? ¿Cuántas con trámite y cuántas sin?
# MAGIC 2. ¿Qué % de las altas pospago tiene el STA_DATETIME heredado?
# MAGIC 3. ¿Cuánto tarda en promedio una portabilidad desde la solicitud hasta la ejecución?
# MAGIC 4. ¿Cuántos números se reciclaron (nuevo cliente) y cuánto tiempo estuvieron libres?
# MAGIC 5. De las líneas que cambiaron de titular, ¿cuántas se dieron de baja en los 90 días siguientes?
# MAGIC
# MAGIC ## Repaso final para el examen
# MAGIC Para cada notebook, vuelve a sus *Preguntas tipo examen* sin mirar las respuestas. Las que falles, relee esa sección del libro.

