# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
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

# MAGIC %sql
# MAGIC SELECT table_schema, count(*) AS columnas_con_comentario
# MAGIC FROM workspace.information_schema.columns
# MAGIC WHERE table_schema IN ('bronze', 'silver', 'gold') AND comment IS NOT NULL
# MAGIC GROUP BY table_schema
# MAGIC ORDER BY table_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.1 · Etiquetar columnas personales
# MAGIC **Tareas:**
# MAGIC 1. Etiqueta en `silver.lineas`: `isdn9` → `pii = 'telefono'`, `cust_id` → `pii = 'cliente'`.
# MAGIC 2. Etiqueta la tabla: `dominio = 'lineas_moviles'`, `clasificacion = 'confidencial'`.
# MAGIC 3. Consulta `information_schema.column_tags` y lista todas las columnas etiquetadas del catálogo.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver.lineas ALTER COLUMN isdn9   SET TAGS ('pii' = 'telefono');
# MAGIC ALTER TABLE silver.lineas ALTER COLUMN cust_id SET TAGS ('pii' = 'cliente');
# MAGIC ALTER TABLE silver.lineas SET TAGS ('dominio' = 'lineas_moviles', 'clasificacion' = 'confidencial');

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
# MAGIC SELECT current_user();    -- debe decir brando.rojas@pucp.edu.pe; si dice otra cosa, usa eso abajo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION gobierno.mascara_numero(x STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Numero completo solo para el propietario; para los demas, los ultimos 4'
# MAGIC RETURN CASE WHEN current_user() = 'brando.rojas@pucp.edu.pe' THEN x ELSE concat('*****', right(x, 4)) END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: ALTER TABLE silver.lineas ALTER COLUMN isdn9 SET MASK gobierno.mascara_numero;
# MAGIC ALTER TABLE silver.lineas ALTER COLUMN isdn9 SET MASK gobierno.mascara_numero;
# MAGIC
# MAGIC SELECT tipo, isdn9 FROM silver.lineas LIMIT 5;      -- tú: número completo

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.3 · Filtro de filas
# MAGIC **Tarea:** crea `gobierno.solo_prepago(tipo STRING)` que deje ver todas las filas a ti y solo `tipo = 'PRE'` a los demás. Aplícalo a `silver.lineas` con `SET ROW FILTER ... ON (tipo)`.
# MAGIC Después, quítalo (`DROP ROW FILTER`) y quita también la máscara (`DROP MASK`) para seguir trabajando.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE OR REPLACE FUNCTION gobierno.solo_prepago(tipo STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'El propietario ve todo; los demas solo prepago'
# MAGIC RETURN current_user() = 'brando.rojas@pucp.edu.pe' OR tipo = 'PRE';
# MAGIC
# MAGIC ALTER TABLE silver.lineas SET ROW FILTER gobierno.solo_prepago ON (tipo);
# MAGIC
# MAGIC SELECT tipo, count(*) AS lineas FROM silver.lineas GROUP BY tipo;     -- tú: PRE y POS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cambia el correo de las dos funciones por uno que no es el tuyo y mira qué ves.
# MAGIC CREATE OR REPLACE FUNCTION gobierno.mascara_numero(x STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN current_user() = 'nadie@x.com' THEN x ELSE concat('*****', right(x, 4)) END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION gobierno.solo_prepago(tipo STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN current_user() = 'nadie@x.com' OR tipo = 'PRE';
# MAGIC
# MAGIC SELECT tipo, isdn9 FROM silver.lineas LIMIT 5;                          -- ahora: *****1234
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tipo, count(*) AS lineas FROM silver.lineas GROUP BY tipo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) las funciones vuelven a tu correo
# MAGIC CREATE OR REPLACE FUNCTION gobierno.mascara_numero(x STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Numero completo solo para el propietario; para los demas, los ultimos 4'
# MAGIC RETURN CASE WHEN current_user() = 'brando.rojas@pucp.edu.pe' THEN x ELSE concat('*****', right(x, 4)) END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION gobierno.solo_prepago(tipo STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'El propietario ve todo; los demas solo prepago'
# MAGIC RETURN current_user() = 'brando.rojas@pucp.edu.pe' OR tipo = 'PRE';
# MAGIC
# MAGIC -- 2) se quitan de la tabla
# MAGIC ALTER TABLE silver.lineas DROP ROW FILTER;
# MAGIC ALTER TABLE silver.lineas ALTER COLUMN isdn9 DROP MASK;
# MAGIC
# MAGIC SELECT tipo, count(*) AS lineas FROM silver.lineas GROUP BY tipo;

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
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.gold TO `account users`;
# MAGIC GRANT SELECT     ON SCHEMA workspace.gold TO `account users`;
# MAGIC SHOW GRANTS ON SCHEMA workspace.gold;          -- ahora aparece account users con USE SCHEMA y SELECT

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE SELECT     ON SCHEMA workspace.gold FROM `account users`;
# MAGIC REVOKE USE SCHEMA ON SCHEMA workspace.gold FROM `account users`;

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

# MAGIC %sql
# MAGIC -- linaje en varios saltos: de quién viene silver.lineas
# MAGIC SELECT DISTINCT source_table_full_name, target_table_full_name
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name IN ('workspace.gold.bajas_por_mes', 'workspace.silver.lineas')
# MAGIC   AND source_table_full_name IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E7.6 · Delta Sharing y Lakehouse Federation (teoría)
# MAGIC Responde en `docs/decisiones.md`, pensando en telco:
# MAGIC 1. Si OSIPTEL pidiera un extracto periódico de altas y bajas, ¿Delta Sharing sería una opción? ¿Qué compartirías (tabla, vista, columnas) y qué **no**?
# MAGIC 2. Si quisieras consultar Oracle REPORT desde Databricks **sin copiar los datos**, ¿qué componente lo permite? ¿Qué limitaciones tendría frente a la exportación a Parquet que hiciste?

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC **Respuestas**
# MAGIC - **OSIPTEL y Delta Sharing**: sí sería opción, con **open sharing** (el destinatario no necesita Databricks; recibe credenciales y lee con un cliente del protocolo abierto). Compartirías una vista o una tabla gold con solo lo pedido (altas y bajas por periodo, con el identificador que exija el requerimiento) y no las tablas bronze, ni columnas de cliente (documento, nombre, dirección), ni IDs internos que no se pidieron. Ojo: una tabla con row filter o column mask no se puede compartir por open sharing; por eso conviene una vista o tabla hecha para compartir.
# MAGIC - **Consultar Oracle REPORT sin copiar**: Lakehouse Federation (una **connection** a Oracle y un **foreign catalog** en Unity Catalog). Limitaciones frente a tu export: cada consulta va a Oracle (carga sobre la base productiva y depende de que esté arriba), el cómputo serverless tiene que llegar por red a una IP interna de Bitel (10.x, que hoy no es alcanzable desde la nube sin conectividad privada), es de solo lectura, no tiene time travel ni las demás funciones de Delta, y el rendimiento depende de qué filtros se empujen a Oracle. El export a Parquet es una foto vieja pero propia, rápida y offline
# MAGIC

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
# MAGIC