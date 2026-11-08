-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06b · Pipeline declarativo `trazabilidad_bitel`
-- MAGIC
-- MAGIC Este notebook **no se corre a mano**: es el código fuente del pipeline (E6.1). Esquema destino: `gold`.
-- MAGIC Si tu catálogo no es `workspace`, cambia las rutas `/Volumes/workspace/...` y los nombres `workspace.silver...`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Bronze: ACTION_AUDIT prepago llega por Auto Loader

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE audit_pre_bronze
COMMENT 'CM_PRE.ACTION_AUDIT tal como llega, un archivo por dia'
AS SELECT *, _metadata.file_path AS _archivo, current_timestamp() AS _ingesta
FROM STREAM read_files('/Volumes/workspace/landing/raw/llegadas/action_audit_pre/', format => 'parquet');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Silver: tipos y expectativas de calidad

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE audit_pre_silver (
  CONSTRAINT fecha_valida    EXPECT (issue_ts IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT codigo_presente EXPECT (ACTION_CODE IS NOT NULL),
  CONSTRAINT en_rango        EXPECT (year(issue_ts) BETWEEN 2000 AND 2030)
)
COMMENT 'ACTION_AUDIT prepago con fecha tipada'
AS SELECT *, try_to_timestamp(ISSUE_DATETIME, 'yyyy-MM-dd HH:mm:ss') AS issue_ts
FROM STREAM(audit_pre_bronze);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Gold: vistas materializadas

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW audit_pre_por_dia
COMMENT 'Eventos por dia y codigo de accion'
AS SELECT date(issue_ts) AS dia, ACTION_CODE, count(*) AS eventos
FROM audit_pre_silver
GROUP BY ALL;

-- COMMAND ----------

-- Lee las tablas batch que armaste en el cap. 4 (silver.lineas, silver.transiciones)
CREATE OR REFRESH MATERIALIZED VIEW bajas_por_mes
COMMENT 'Lineas dadas de baja por mes y tipo (segun END_DATETIME)'
AS SELECT tipo, date_trunc('month', baja_ts) AS mes, count(*) AS bajas
FROM workspace.silver.lineas
WHERE baja_ts IS NOT NULL
GROUP BY ALL;

-- COMMAND ----------

-- TODO (E6.1): descomenta cuando exista silver.transiciones (E4.4)
-- CREATE OR REFRESH MATERIALIZED VIEW migraciones_por_mes
-- COMMENT 'Transiciones por mes, sentido y si es el mismo cliente'
-- AS SELECT date_trunc('month', inicio_ts) AS mes, transicion, mismo_cliente, count(*) AS n
-- FROM workspace.silver.transiciones
-- GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Historia de estados de cada línea (AUTO CDC, SCD tipo 2) · E6.4
-- MAGIC Ajusta `SUB_ID`, `STATUS` y `DATE_CHANGE` a los nombres reales de CM_PRE.STATUS.
-- MAGIC Si tu versión no reconoce `AUTO CDC`, usa el nombre anterior: `APPLY CHANGES INTO`.

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING TABLE status_pre_bronze
-- AS SELECT *, try_to_timestamp(DATE_CHANGE, 'yyyy-MM-dd HH:mm:ss') AS cambio_ts
-- FROM STREAM read_files('/Volumes/workspace/landing/raw/llegadas/status_pre/', format => 'parquet');
--
-- CREATE OR REFRESH STREAMING TABLE estado_linea_pre_scd2;
--
-- CREATE FLOW estado_linea_pre_flow AS AUTO CDC INTO estado_linea_pre_scd2
-- FROM STREAM(status_pre_bronze)
-- KEYS (SUB_ID)
-- SEQUENCE BY cambio_ts
-- COLUMNS SUB_ID, STATUS, cambio_ts
-- STORED AS SCD TYPE 2;
