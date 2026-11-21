-- 06b · Pipeline declarativo trazabilidad_bitel
-- Archivo SQL del pipeline (no es un notebook): el pipeline lo lee, no se corre a mano.
-- Catálogo y esquema por defecto del pipeline: workspace.gold.
-- Si tu catálogo no es workspace, cambia las rutas /Volumes/workspace/... y los nombres workspace.silver...

-- ═══ 1. Bronze: ACTION_AUDIT prepago llega por Auto Loader ═══════════════════════

CREATE OR REFRESH STREAMING TABLE audit_pre_bronze
COMMENT 'CM_PRE.ACTION_AUDIT tal como llega, un archivo por dia'
AS SELECT *, _metadata.file_path AS _archivo, current_timestamp() AS _ingesta
FROM STREAM read_files('/Volumes/workspace/landing/raw/llegadas/action_audit_pre/', format => 'parquet');

-- ═══ 2. Silver: tipos y expectativas de calidad ══════════════════════════════════

CREATE OR REFRESH STREAMING TABLE audit_pre_silver (
  CONSTRAINT fecha_valida    EXPECT (issue_ts IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT codigo_presente EXPECT (ACTION_CODE IS NOT NULL),
  CONSTRAINT en_rango        EXPECT (year(issue_ts) BETWEEN 2000 AND 2030)
)
COMMENT 'ACTION_AUDIT prepago con fecha tipada'
AS SELECT *, try_to_timestamp(ISSUE_DATETIME, 'yyyy-MM-dd HH:mm:ss') AS issue_ts
FROM STREAM(audit_pre_bronze);

-- ═══ 3. Gold: vistas materializadas ══════════════════════════════════════════════

CREATE OR REFRESH MATERIALIZED VIEW audit_pre_por_dia
COMMENT 'Eventos por dia y codigo de accion'
AS SELECT date(issue_ts) AS dia, ACTION_CODE, count(*) AS eventos
FROM audit_pre_silver
GROUP BY ALL;

-- Estas dos leen las tablas batch del cap. 4 (silver.lineas, silver.transiciones)
CREATE OR REFRESH MATERIALIZED VIEW bajas_por_mes
COMMENT 'Lineas dadas de baja por mes y tipo (segun END_DATETIME)'
AS SELECT tipo, date_trunc('month', baja_ts) AS mes, count(*) AS bajas
FROM workspace.silver.lineas
WHERE baja_ts IS NOT NULL
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW migraciones_por_mes
COMMENT 'Transiciones por mes, sentido y si es el mismo cliente'
AS SELECT date_trunc('month', inicio_ts) AS mes, transicion, mismo_cliente, count(*) AS n
FROM workspace.silver.transiciones
GROUP BY ALL;

-- ═══ 4. Historia de estados de cada número prepago (AUTO CDC, SCD tipo 2) · E6.4 ══
-- CM_PRE.STATUS no tiene SUB_ID (visto en E5.5): la llave es el número normalizado y el estado es NEW_STATUS.
-- AUTO CDC no acepta SEQUENCE BY nulo: esas filas se botan antes con la expectativa.
-- Si tu versión no reconoce AUTO CDC, usa el nombre anterior: APPLY CHANGES INTO.

CREATE OR REFRESH STREAMING TABLE status_pre_bronze (
  CONSTRAINT cambio_valido EXPECT (cambio_ts IS NOT NULL AND isdn9 IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'CM_PRE.STATUS tal como llega, con el numero normalizado y la fecha del cambio tipada'
AS SELECT *,
          nullif(CASE WHEN ISDN RLIKE '^[0-9 +-]+$' THEN right(regexp_replace(ISDN, '[^0-9]', ''), 9) ELSE ISDN END, '') AS isdn9,
          try_to_timestamp(left(DATE_CHANGE, 19), 'yyyy-MM-dd HH:mm:ss') AS cambio_ts
FROM STREAM read_files('/Volumes/workspace/landing/raw/llegadas/status_pre/', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE estado_linea_pre_scd2
COMMENT 'Un periodo de estado por fila (SCD tipo 2) de cada numero prepago';

CREATE FLOW estado_linea_pre_flow AS AUTO CDC INTO estado_linea_pre_scd2
FROM STREAM(status_pre_bronze)
KEYS (isdn9)
SEQUENCE BY cambio_ts
COLUMNS isdn9, OLD_STATUS, NEW_STATUS, cambio_ts
STORED AS SCD TYPE 2
TRACK HISTORY ON NEW_STATUS;