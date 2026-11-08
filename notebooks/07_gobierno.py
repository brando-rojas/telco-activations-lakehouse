# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Capítulo 7: gobierno de datos con Unity Catalog
# MAGIC
# MAGIC **Dominio del examen:** Data Governance & Quality.
# MAGIC **Tiempo:** 2 h. **Lee antes:** cap. 7 (privilegios, `GRANT`/`REVOKE`, propietarios, column masks, row filters, etiquetas, linaje, tablas de sistema, Delta Sharing, Lakehouse Federation).
# MAGIC
# MAGIC Este capítulo es tu terreno: protección de datos aplicada con las herramientas de la plataforma.
# MAGIC En Free Edition **no hay consola de cuenta ni grupos de cuenta**: los `GRANT` a grupos los practicas como sintaxis, y las máscaras las condicionas a tu propio usuario con `current_user()`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.1 · Etiquetar columnas personales
# MAGIC **Tareas:**
# MAGIC 1. Etiqueta en `silver.lineas`: `isdn9` → `pii = 'telefono'`, `cust_id` → `pii = 'cliente'`.
# MAGIC 2. Etiqueta la tabla: `dominio = 'lineas_moviles'`, `clasificacion = 'confidencial'`.
# MAGIC 3. Consulta `information_schema.column_tags` y lista todas las columnas etiquetadas del catálogo.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver.lineas ALTER COLUMN isdn9 SET TAGS ('pii' = 'telefono');
# MAGIC -- TODO: cust_id y las etiquetas de la tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT schema_name, table_name, column_name, tag_name, tag_value
# MAGIC FROM information_schema.column_tags
# MAGIC ORDER BY 1, 2, 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.2 · Máscara de columna
# MAGIC **Tareas:**
# MAGIC 1. Crea la función `gobierno.mascara_numero(x STRING)` que devuelva el valor completo solo a ti (`current_user()` = tu correo) y a todos los demás `'*****' || right(x, 4)`.
# MAGIC 2. Aplícala a `silver.lineas.isdn9` con `ALTER TABLE ... ALTER COLUMN ... SET MASK`.
# MAGIC 3. Prueba: consulta como tú; luego cambia temporalmente el correo en la función por otro y vuelve a consultar.
# MAGIC
# MAGIC **Responde:** ¿qué ventaja tiene esto frente a crear una vista con la columna enmascarada?

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION gobierno.mascara_numero(x STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN current_user() = 'TU_CORREO@dominio.com' THEN x ELSE concat('*****', right(x, 4)) END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: ALTER TABLE silver.lineas ALTER COLUMN isdn9 SET MASK gobierno.mascara_numero;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.3 · Filtro de filas
# MAGIC **Tarea:** crea `gobierno.solo_prepago(tipo STRING)` que deje ver todas las filas a ti y solo `tipo = 'PRE'` a los demás. Aplícalo a `silver.lineas` con `SET ROW FILTER ... ON (tipo)`.
# MAGIC Después, quítalo (`DROP ROW FILTER`) y quita también la máscara (`DROP MASK`) para seguir trabajando.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.4 · Privilegios
# MAGIC **Tareas:**
# MAGIC 1. `SHOW GRANTS ON SCHEMA gold`.
# MAGIC 2. Escribe (aunque en Free Edition falle por no haber grupos) los comandos para que el grupo `analistas_pmo` pueda **leer** todo `gold` y **nada** de `bronze`:
# MAGIC    `GRANT USE CATALOG`, `GRANT USE SCHEMA`, `GRANT SELECT` en el lugar correcto.
# MAGIC 3. ¿Qué privilegio necesita alguien para crear tablas en `silver`? ¿Y para leer archivos de un volumen?

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA gold

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: los GRANT para analistas_pmo (como práctica de sintaxis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.5 · Linaje
# MAGIC **Tareas:**
# MAGIC 1. En **Catalog Explorer**, abre `gold.bajas_por_mes` (cap. 6) → pestaña **Lineage**: ¿de qué tablas viene? ¿Llega hasta bronze?
# MAGIC 2. Si tu workspace tiene tablas de sistema, consulta el linaje con SQL (si no existen, anótalo como límite de Free Edition).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Puede no estar disponible en Free Edition
# MAGIC SELECT source_table_full_name, target_table_full_name, event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name LIKE '%bajas_por_mes%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.6 · Delta Sharing y Lakehouse Federation (teoría)
# MAGIC Responde en `docs/decisiones.md`, pensando en telco:
# MAGIC 1. Si OSIPTEL pidiera un extracto periódico de altas y bajas, ¿Delta Sharing sería una opción? ¿Qué compartirías (tabla, vista, columnas) y qué **no**?
# MAGIC 2. Si quisieras consultar Oracle REPORT desde Databricks **sin copiar los datos**, ¿qué componente lo permite? ¿Qué limitaciones tendría frente a la exportación a Parquet que hiciste?

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.7 · Tu política de datos en el repo
# MAGIC Escribe `docs/politica_datos.md` (1 página): qué columnas consideras personales, cómo las seudonimizaste antes de subirlas, qué máscaras y filtros aplicarías en un workspace corporativo, y por qué los datos nunca entran al repo.
# MAGIC Es el documento que mostrarías en una entrevista para explicar cómo combinas ingeniería de datos y protección de datos.
# MAGIC
# MAGIC ## Preguntas tipo examen
# MAGIC 1. ¿Cuál es el orden mínimo de privilegios para que alguien lea una tabla: `SELECT` solamente, o algo más?
# MAGIC 2. ¿Qué diferencia hay entre un column mask y una vista dinámica?
# MAGIC 3. ¿Quién puede otorgar privilegios sobre una tabla?
# MAGIC 4. ¿Qué componente usarías para compartir datos con otra organización que **no** usa Databricks?
# MAGIC 5. ¿Dónde consultas quién accedió a qué tabla?
# MAGIC
# MAGIC <details><summary>Respuestas</summary>
# MAGIC
# MAGIC 1. `USE CATALOG` en el catálogo, `USE SCHEMA` en el esquema y `SELECT` en la tabla.
# MAGIC 2. La máscara se aplica sobre la tabla misma: cualquiera que la consulte (SQL, notebook, BI) recibe el valor enmascarado. La vista dinámica obliga a que todos usen la vista y no la tabla.
# MAGIC 3. Su propietario, quien tenga `MANAGE` sobre ella, o los administradores del metastore/catálogo.
# MAGIC 4. Delta Sharing abierto (protocolo abierto, no requiere Databricks del otro lado).
# MAGIC 5. En las tablas de sistema de auditoría (`system.access.audit`).
# MAGIC </details>

