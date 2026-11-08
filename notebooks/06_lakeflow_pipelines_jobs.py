# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Capítulo 6: pipelines de producción (Lakeflow)
# MAGIC
# MAGIC **Dominio del examen:** Productionizing Data Pipelines.
# MAGIC **Tiempo:** 3 h. **Lee antes:** cap. 6. En la guía del examen de agosto 2026 los nombres son nuevos:
# MAGIC *Delta Live Tables* → **Lakeflow Declarative Pipelines**; *Workflows* → **Lakeflow Jobs**; `APPLY CHANGES` → **`AUTO CDC`**.
# MAGIC
# MAGIC Construyes dos cosas:
# MAGIC 1. Un **pipeline declarativo** (`06b_pipeline_trazabilidad.sql`): ACTION_AUDIT llega por Auto Loader → silver con **expectativas** de calidad → gold con vistas materializadas de bajas y migraciones.
# MAGIC 2. Un **job** de 3 tareas: llegan días nuevos → corre el pipeline → corre un control de calidad.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## E6.1 · Crear y correr el pipeline
# MAGIC 1. **Jobs & Pipelines → Create → ETL pipeline.**
# MAGIC 2. Nombre `trazabilidad_telco`. Código fuente: `06b_pipeline_trazabilidad.sql` (este mismo Git folder).
# MAGIC 3. Catálogo `workspace`, esquema por defecto **`gold`**. Serverless. Modo **Triggered**.
# MAGIC 4. Antes de correrlo, asegúrate de que hay días en llegadas (`llegan(5)` del cap. 5, o la tarea `06c` de abajo).
# MAGIC 5. Corre el pipeline y abre el grafo: ¿qué tablas son *streaming tables* y cuáles *materialized views*?
# MAGIC
# MAGIC **Terminado cuando:** el pipeline termina en verde y ves las métricas de expectativas (filas que pasaron y que se descartaron) en la tabla `audit_pre_silver`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E6.2 · Leer el event log del pipeline
# MAGIC El event log guarda cada corrida, cada flujo y las métricas de calidad. **Tarea:** con la consulta de abajo, saca por corrida cuántas filas pasaron y fallaron cada expectativa.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, details:flow_progress:data_quality:expectations AS expectativas
# MAGIC FROM event_log(TABLE(gold.audit_pre_silver))
# MAGIC WHERE event_type = 'flow_progress' AND details:flow_progress:data_quality IS NOT NULL
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E6.3 · Cambiar la acción de una expectativa
# MAGIC En `06b`, la expectativa `fecha_valida` tiene `ON VIOLATION DROP ROW`.
# MAGIC **Tareas:** cámbiala a (a) sin acción (solo registra) y (b) `FAIL UPDATE`. Corre el pipeline con cada una y anota qué pasa.
# MAGIC **Responde:** ¿cuál usarías para un dato que **no puede** faltar en un reporte regulatorio y cuál para uno que solo quieres vigilar?

# COMMAND ----------

# MAGIC %md
# MAGIC ## E6.4 · Historia de estados con AUTO CDC (SCD tipo 2)
# MAGIC La sección 4 de `06b` arma `estado_linea_pre_scd2`: una fila por **periodo de estado** de cada línea, con `__START_AT` y `__END_AT`, a partir de CM_PRE.STATUS.
# MAGIC **Tareas:**
# MAGIC 1. Ajusta los nombres de columna de esa sección a los reales (los viste en E5.5).
# MAGIC 2. Haz llegar días de STATUS a `llegadas/status_pre/` (copia la idea de `llegan()` con `ORIGEN = "CM_PRE.STATUS"`).
# MAGIC 3. Corre el pipeline y consulta la historia de una línea con varios cambios.
# MAGIC
# MAGIC **Terminado cuando:** una línea con 3 cambios tiene 3 filas, y solo la última tiene `__END_AT` nulo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E6.5 · Job de 3 tareas
# MAGIC 1. **Jobs & Pipelines → Create → Job** `job_trazabilidad`.
# MAGIC 2. Tarea 1 `llegada`: notebook `06c_tarea_llegada` con parámetro `dias = 3`.
# MAGIC 3. Tarea 2 `pipeline`: tipo *Pipeline*, `trazabilidad_telco`, depende de `llegada`.
# MAGIC 4. Tarea 3 `control`: notebook `06d_tarea_control`, depende de `pipeline`. Si encuentra problemas, falla.
# MAGIC 5. Reintentos: 1 en la tarea 1. Notificación por correo si falla.
# MAGIC 6. Córrelo dos veces. En la segunda, fuerza un fallo en `control` (bájale el umbral) y usa **Repair run** para reintentar solo esa tarea.
# MAGIC
# MAGIC **No lo programes cada pocos minutos:** la cuota diaria de Free Edition se agota. Una programación diaria y pausada basta para practicar.
# MAGIC
# MAGIC **Terminado cuando:** tienes una corrida verde completa y una reparada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E6.6 · Para el examen: Asset Bundles y despliegue
# MAGIC No necesitas hacerlo en Free Edition, pero sí saber qué es: un **Databricks Asset Bundle** describe en YAML los jobs, pipelines y notebooks de un proyecto para desplegarlos igual en dev, test y prod (`databricks bundle validate / deploy / run`).
# MAGIC **Tarea opcional:** escribe a mano el `databricks.yml` que describiría `job_trazabilidad` y súbelo al repo.
# MAGIC
# MAGIC ## Preguntas tipo examen
# MAGIC 1. ¿Qué diferencia hay entre una *streaming table* y una *materialized view* en un pipeline declarativo?
# MAGIC 2. Una expectativa con `ON VIOLATION FAIL UPDATE` encuentra una fila mala. ¿Qué pasa con la actualización?
# MAGIC 3. ¿Qué modo de pipeline usarías para correrlo una vez al día: *triggered* o *continuous*?
# MAGIC 4. Una tarea de un job falló y las anteriores estaban bien. ¿Cómo la reintentas sin repetir todo?
# MAGIC 5. ¿Qué guarda `AUTO CDC ... STORED AS SCD TYPE 2` que no guarda el tipo 1?
# MAGIC
# MAGIC <details><summary>Respuestas</summary>
# MAGIC
# MAGIC 1. La streaming table procesa cada registro una sola vez (incremental, para fuentes que solo agregan). La materialized view guarda el resultado de una consulta y se recalcula (incremental cuando puede).
# MAGIC 2. Falla la actualización y no se escribe nada de ese flujo.
# MAGIC 3. Triggered.
# MAGIC 4. Con *Repair run*: solo corre las tareas que fallaron y las que dependen de ellas.
# MAGIC 5. La historia: cada versión del registro con su vigencia (`__START_AT`, `__END_AT`). El tipo 1 sobrescribe y guarda solo lo último.
# MAGIC </details>

