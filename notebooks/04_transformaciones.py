# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 · Capítulo 4: transformar datos con Spark
# MAGIC
# MAGIC **Dominio del examen:** Data Processing & Transformations (el de más peso).
# MAGIC **Tiempo:** 4–5 h, en dos sesiones. **Lee antes:** cap. 4 (tipos y fechas, joins, ventanas, funciones de orden superior, `explode`, `pivot`, UDF vs funciones nativas).
# MAGIC
# MAGIC Este es el notebook donde respondes tus preguntas de negocio:
# MAGIC - **¿En qué sentido migró cada línea y cuándo de verdad?** (PRE→POS, POS→PRE)
# MAGIC - **¿Cuánto se hereda el STA_DATETIME?** (y por qué ordenar por alta engaña)
# MAGIC - **¿El titular actual coincide con el último cambio de titularidad?**
# MAGIC
# MAGIC Reglas de negocio que ya validaste y que los ejercicios usan:
# MAGIC - **Cliente.** Pospago: línea → contrato → cliente (`CM_POS.SUB_MB → CM_POS.CONTRACT → CM_POS.CUSTOMER`). Prepago: `CM_PRE.SUB_MB.CUST_ID → CM_POS.CUSTOMER`.
# MAGIC - **STATUS** no tiene catálogo en AP_DOMAIN; sus significados están en `gold.catalogo_valores` (E3.6). 2 activa y 3 cancelada valen para los dos orígenes; 0, 1, 4 y 9 cambian:
# MAGIC   - prepago: 0 eliminada (kit sin usar o pasó a pospago), 1 registrada sin conectar, 4 cancelada en el cierre masivo de 2019;
# MAGIC   - pospago: 0 pasó a prepago, 1 registrada sin conectar, 4 cerrada por cambio de titular, 9 preactivada.
# MAGIC   - Una línea cerrada puede tener STATUS 0, 3 o 4.
# MAGIC - **ACT_STATUS:** un bloqueo por dígito. 1.º pedido por el cliente; 2.º deuda (pospago) o sistema (prepago); 3.º límite de crédito (solo pospago). 0 sin bloqueo, 1 una vía (salientes), 2 dos vías. En prepago, 2.º dígito 3 = kit preactivado (STATUS 2 pero nunca usado) y 4 = sin saldo. En palabras: `silver.f_bloqueo`; estado final: `silver.f_estado_linea`.
# MAGIC - El STA_DATETIME **se hereda** en la migración pre↔pos.
# MAGIC - **Hito de alta** en ACTION_AUDIT: ACTION_CODE `'00'`, a 15 minutos como máximo del STA_DATETIME. En pospago, las altas que entran por `'666'` (Receive active subscriber) reciben el `'00'` después; STATUS 9 = tiene `'666'` y todavía no tiene `'00'`.
# MAGIC - **Migración:** `'00'` con REASON_ID 6982393 (PRE→POS) o 31000140 (POS→PRE). La línea de origen se cierra con `'167'` (en CM_POS «a prepago», en CM_PRE «a pospago»).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda nueva al inicio del notebook 04: ts() e isdn9() del config, pero como funciones SQL de Unity Catalog.
# MAGIC -- Así se pueden usar en celdas %sql (las de 00_config son de Python y solo sirven dentro de f-strings).
# MAGIC CREATE OR REPLACE FUNCTION silver.f_ts(x STRING)
# MAGIC RETURNS TIMESTAMP
# MAGIC COMMENT 'Texto yyyy-MM-dd HH:mm:ss -> timestamp (ignora fracciones). Fuera de 2000-2030 -> NULL'
# MAGIC RETURN CASE WHEN year(try_to_timestamp(left(x, 19), 'yyyy-MM-dd HH:mm:ss')) BETWEEN 2000 AND 2030
# MAGIC             THEN try_to_timestamp(left(x, 19), 'yyyy-MM-dd HH:mm:ss') END;
# MAGIC  
# MAGIC CREATE OR REPLACE FUNCTION silver.f_isdn9(x STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Numero normalizado: ultimos 9 digitos si es numero en claro; si viene seudonimizado, igual'
# MAGIC RETURN nullif(CASE WHEN x RLIKE '^[0-9 +-]+$' THEN right(regexp_replace(x, '[^0-9]', ''), 9) ELSE x END, '');

# COMMAND ----------

def col_o_nulo(tabla, columna):
    """La columna si existe en la tabla; si no, NULL (así el SQL no se cae si una tabla no la tiene)."""
    return columna if columna.upper() in [c.upper() for c in spark.table(tabla).columns] else "CAST(NULL AS STRING)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.1 · `silver.lineas`: prepago y pospago en una sola tabla tipada
# MAGIC **Tareas:**
# MAGIC 1. Completa la parte de pospago (el cliente sale del **contrato**, no de SUB_MB).
# MAGIC 2. Une las dos con `unionByName`.
# MAGIC 3. Escribe `silver.lineas` y cuenta filas por `tipo`: deben sumar lo mismo que las dos bronze.
# MAGIC
# MAGIC **Terminado cuando:** los conteos cuadran y `sub_id` es único dentro de cada `tipo` (celda de chequeo).

# COMMAND ----------

t_pre, t_pos, t_con = tb("CM_PRE.SUB_MB"), tb("CM_POS.SUB_MB"), tb("CM_POS.CONTRACT")

pre = spark.sql(f"""
  SELECT 'PRE' AS tipo,
         {COL['sub_id']}                        AS sub_id,
         {isdn9(COL['isdn'])}                   AS isdn9,
         {col_o_nulo(t_pre, COL['cust_id'])}    AS cust_id,
         CAST(NULL AS STRING)                   AS contract_id,
         {ts(COL['alta'])}                      AS alta_ts,
         {ts(COL['baja'])}                      AS baja_ts,
         {ts(col_o_nulo(t_pre, COL['migracion']))} AS mig_ts,
         {COL['status']}                        AS status,
         {COL['act_status']}                    AS act_status,
         {COL['alta']}                          AS alta_txt,
         {COL['baja']}                          AS baja_txt
  FROM {t_pre}""")

# TODO: pos = spark.sql(...) igual que pre, pero:
#   - tipo 'POS'
#   - contract_id = SUB_MB.CONTRACT_ID
#   - cust_id = el CUST_ID del contrato (LEFT JOIN {t_con} c ON c.CONTRACT_ID = s.CONTRACT_ID)
#   - prefija las columnas con s. para que no choquen con las del contrato
mig_pos = (f"s.{COL['migracion']}"
           if col_o_nulo(t_pos, COL['migracion']) != "CAST(NULL AS STRING)" else "CAST(NULL AS STRING)")
 
pos = spark.sql(f"""
  SELECT 'POS' AS tipo,
         s.{COL['sub_id']}                      AS sub_id,
         {isdn9('s.' + COL['isdn'])}            AS isdn9,
         c.{COL['contrato_cliente']}            AS cust_id,
         s.{COL['contract_id']}                 AS contract_id,
         {ts('s.' + COL['alta'])}               AS alta_ts,
         {ts('s.' + COL['baja'])}               AS baja_ts,
         {ts(mig_pos)}                          AS mig_ts,
         s.{COL['status']}                      AS status,
         s.{COL['act_status']}                  AS act_status,
         s.{COL['alta']}                        AS alta_txt,
         s.{COL['baja']}                        AS baja_txt
  FROM {t_pos} s
  LEFT JOIN (SELECT {COL['contract_id']}, max({COL['contrato_cliente']}) AS {COL['contrato_cliente']}
             FROM {t_con} GROUP BY {COL['contract_id']}) c     -- un contrato = una fila: el join no duplica líneas
         ON c.{COL['contract_id']} = s.{COL['contract_id']}""")


lineas = pre.unionByName(pos)
lineas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.lineas")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tipo, count(*) AS filas, count(DISTINCT sub_id) AS sub_ids,
# MAGIC        count(*) - count(DISTINCT sub_id) AS repetidos
# MAGIC FROM silver.lineas GROUP BY tipo

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.2 · Calidad de las fechas
# MAGIC **Tareas:** para cada tipo, cuenta:
# MAGIC - fechas en texto que **no** se pudieron convertir (`alta_txt` no nulo y `alta_ts` nulo), igual para baja;
# MAGIC - líneas con `baja_ts < alta_ts`;
# MAGIC - líneas canceladas (STATUS 3) **sin** fecha de baja, y activas (STATUS 2) **con** fecha de baja;
# MAGIC - líneas cerradas con STATUS 0 o 4 **sin** fecha de baja (también son cierres);
# MAGIC - kits de prepago: STATUS 2 con `act_status` que contiene 3.
# MAGIC
# MAGIC Después parte los cierres sin fecha con `silver.f_estado_linea`: un prepago 0 con `03` y sin fechas es un kit borrado antes de usarse, no un error del dato.
# MAGIC
# MAGIC **Terminado cuando:** tienes una tabla de 2 filas (PRE, POS) con esos 7 conteos. Cada número distinto de cero es un hallazgo para el diccionario.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tipo,
# MAGIC        count_if(alta_txt IS NOT NULL AND alta_ts IS NULL)          AS alta_no_convertible,
# MAGIC        count_if(baja_txt IS NOT NULL AND baja_ts IS NULL)          AS baja_no_convertible,
# MAGIC        count_if(baja_ts < alta_ts)                                 AS baja_antes_de_alta,
# MAGIC        count_if(status = '3' AND baja_ts IS NULL)                  AS cancelada_sin_baja,
# MAGIC        count_if(status IN ('0', '4') AND baja_ts IS NULL)          AS cerrada_0_4_sin_baja,   -- también son cierres (ver E3.6)
# MAGIC        count_if(status = '2' AND baja_ts IS NOT NULL)              AS activa_con_baja,
# MAGIC        count_if(tipo = 'PRE' AND status = '2' AND act_status LIKE '%3%') AS pre_kits_preactivados,
# MAGIC        count(*)                                                    AS total
# MAGIC FROM silver.lineas
# MAGIC GROUP BY tipo
# MAGIC ORDER BY tipo;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- los cierres sin fecha, por estado final (prepago 0 con 03 = kit borrado sin usarse: no es error)
# MAGIC SELECT tipo, status, act_status, silver.f_estado_linea(tipo, status, act_status) AS estado_final, count(*) AS lineas
# MAGIC FROM silver.lineas
# MAGIC WHERE status IN ('0', '3', '4') AND baja_ts IS NULL
# MAGIC GROUP BY ALL
# MAGIC ORDER BY lineas DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- si alta_no_convertible > 0: mira qué forma tienen esos textos
# MAGIC SELECT tipo, alta_txt, count(*) AS n
# MAGIC FROM silver.lineas
# MAGIC WHERE alta_txt IS NOT NULL AND alta_ts IS NULL
# MAGIC GROUP BY tipo, alta_txt ORDER BY n DESC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.3 · La trampa de ordenar por alta
# MAGIC La idea natural es ordenar todos los periodos de un número por `alta_ts` y mirar el periodo anterior con `LAG`.
# MAGIC Si el STA_DATETIME se hereda, la línea pospago nueva tiene **la misma alta** que la prepago que migró: el orden queda empatado y la transición sale al revés o al azar.
# MAGIC
# MAGIC **Tareas:**
# MAGIC 1. Cuenta los **empates**: pares PRE/POS con el mismo `isdn9` y `alta_ts` a menos de 1 minuto.
# MAGIC 2. De esos empates, ¿cuántos tienen el **mismo cliente**? (eso apunta a migración y no a reciclaje del número)
# MAGIC 3. Mira 20 casos con todas sus fechas (alta, baja, mig) lado a lado.
# MAGIC
# MAGIC **Terminado cuando:** sabes qué porcentaje de las líneas pospago tiene un "gemelo" prepago con la misma alta. Ese porcentaje es tu primera medida del STA heredado.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pista: self-join de silver.lineas consigo misma por isdn9, un lado tipo='PRE' y el otro tipo='POS',
# MAGIC --        con abs(unix_timestamp(pos.alta_ts) - unix_timestamp(pre.alta_ts)) < 60
# MAGIC -- TODO
# MAGIC CREATE OR REPLACE TABLE silver.gemelos
# MAGIC COMMENT 'Pares PRE/POS del mismo numero con STA_DATETIME a menos de 60 s (STA heredado)'
# MAGIC AS
# MAGIC SELECT pre.isdn9,
# MAGIC        pre.sub_id  AS sub_pre,   pos.sub_id  AS sub_pos,
# MAGIC        pre.cust_id AS cust_pre,  pos.cust_id AS cust_pos,
# MAGIC        pre.cust_id = pos.cust_id AS mismo_cliente,
# MAGIC        pre.alta_ts AS alta_pre,  pos.alta_ts AS alta_pos,
# MAGIC        pre.baja_ts AS baja_pre,  pos.baja_ts AS baja_pos,
# MAGIC        pre.mig_ts  AS mig_pre,   pos.mig_ts  AS mig_pos,
# MAGIC        pre.status  AS status_pre, pos.status AS status_pos,
# MAGIC        CASE WHEN pos.mig_ts IS NOT NULL AND pre.mig_ts IS NULL THEN 'PRE→POS'
# MAGIC             WHEN pre.mig_ts IS NOT NULL AND pos.mig_ts IS NULL THEN 'POS→PRE'
# MAGIC             WHEN pre.baja_ts < coalesce(pos.baja_ts, TIMESTAMP'9999-12-31') THEN 'PRE→POS'
# MAGIC             WHEN pos.baja_ts < coalesce(pre.baja_ts, TIMESTAMP'9999-12-31') THEN 'POS→PRE'
# MAGIC        END AS sentido_probable
# MAGIC FROM silver.lineas pre
# MAGIC JOIN silver.lineas pos
# MAGIC   ON pos.isdn9 = pre.isdn9
# MAGIC  AND pre.tipo = 'PRE' AND pos.tipo = 'POS'
# MAGIC  AND abs(unix_timestamp(pos.alta_ts) - unix_timestamp(pre.alta_ts)) < 60;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) y 2) empates, mismo cliente y % de pospago con gemelo
# MAGIC SELECT count(*)                                         AS pares,
# MAGIC        count_if(mismo_cliente)                          AS pares_mismo_cliente,
# MAGIC        round(100 * count_if(mismo_cliente) / count(*), 1) AS pct_mismo_cliente,
# MAGIC        count(DISTINCT sub_pos)                          AS pos_con_gemelo,
# MAGIC        round(100 * count(DISTINCT sub_pos)
# MAGIC              / (SELECT count(*) FROM silver.lineas WHERE tipo = 'POS'), 2) AS pct_pos_con_gemelo
# MAGIC FROM silver.gemelos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) 20 casos con todas sus fechas
# MAGIC SELECT isdn9, sentido_probable, mismo_cliente, alta_pre, baja_pre, mig_pre, alta_pos, baja_pos, mig_pos
# MAGIC FROM silver.gemelos
# MAGIC ORDER BY isdn9
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.4 · Transiciones con ventanas (bien hecho)
# MAGIC Ahora define un **inicio efectivo** de cada periodo que no dependa del STA heredado. Opciones (elige y justifica en `docs/decisiones.md`):
# MAGIC - para un POS con gemelo PRE: la `baja_ts` del PRE (el prepago se cierra cuando nace el pospago);
# MAGIC - o `mig_ts` si viene lleno y es coherente;
# MAGIC - o la fecha del trámite en `CM_POS.TRANSFER_REQUEST` / `CM_PRE.REQUEST_TRANSFER_POSTPAID` (E4.5).
# MAGIC
# MAGIC **Tareas:**
# MAGIC 1. Agrega `inicio_ts` a cada periodo según tu regla.
# MAGIC 2. Con `LAG` sobre `PARTITION BY isdn9 ORDER BY inicio_ts`, arma `silver.transiciones`: transición (`PRE→POS`, `POS→PRE`, `PRE→PRE`, `POS→POS`), días entre la baja anterior y el inicio nuevo, y si es el mismo cliente.
# MAGIC 3. Resume: transiciones por tipo × mismo_cliente × rango de días (0, 1–3, 4–30, >30).
# MAGIC
# MAGIC **Cómo leerlo:** `PRE→POS` con mismo cliente y 0–3 días = migración. Cambio de cliente con muchos días = número **reciclado** (no es migración).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE TABLE silver.transiciones AS
# MAGIC -- WITH periodos AS (SELECT *, <tu regla> AS inicio_ts FROM silver.lineas WHERE isdn9 IS NOT NULL),
# MAGIC --      con_anterior AS (SELECT *, LAG(tipo) OVER w AS tipo_ant, LAG(baja_ts) OVER w AS baja_ant,
# MAGIC --                              LAG(cust_id) OVER w AS cust_ant, LAG(sub_id) OVER w AS sub_ant
# MAGIC --                       FROM periodos WINDOW w AS (PARTITION BY isdn9 ORDER BY inicio_ts, sub_id))
# MAGIC -- SELECT ... FROM con_anterior WHERE tipo_ant IS NOT NULL
# MAGIC -- 1) inicio efectivo de cada periodo
# MAGIC CREATE OR REPLACE TABLE silver.periodos
# MAGIC COMMENT 'silver.lineas + inicio efectivo del periodo (no depende del STA heredado)'
# MAGIC AS
# MAGIC WITH gemelo AS (
# MAGIC   -- la baja del periodo de OTRO tipo, misma alta (±60 s), que se cerró antes que este
# MAGIC   SELECT n.tipo, n.sub_id, max(o.baja_ts) AS baja_gemelo
# MAGIC   FROM silver.lineas n
# MAGIC   JOIN silver.lineas o
# MAGIC     ON o.isdn9 = n.isdn9 AND o.tipo <> n.tipo
# MAGIC    AND abs(unix_timestamp(o.alta_ts) - unix_timestamp(n.alta_ts)) < 60
# MAGIC    AND o.baja_ts IS NOT NULL
# MAGIC    AND (n.baja_ts IS NULL OR o.baja_ts < n.baja_ts)
# MAGIC   GROUP BY n.tipo, n.sub_id),
# MAGIC regla AS (
# MAGIC   SELECT l.*, g.baja_gemelo,
# MAGIC          (l.mig_ts >= l.alta_ts AND (l.baja_ts IS NULL OR l.mig_ts <= l.baja_ts)) AS mig_coherente
# MAGIC   FROM silver.lineas l
# MAGIC   LEFT JOIN gemelo g ON g.tipo = l.tipo AND g.sub_id = l.sub_id
# MAGIC   WHERE l.isdn9 IS NOT NULL)
# MAGIC SELECT *,
# MAGIC        CASE WHEN mig_coherente THEN mig_ts
# MAGIC             WHEN baja_gemelo IS NOT NULL THEN baja_gemelo
# MAGIC             ELSE alta_ts END AS inicio_ts,
# MAGIC        CASE WHEN mig_coherente THEN 'MIG_DATETIME'
# MAGIC             WHEN baja_gemelo IS NOT NULL THEN 'BAJA_GEMELO'
# MAGIC             ELSE 'STA_DATETIME' END AS fuente_inicio
# MAGIC FROM regla;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) transiciones con LAG
# MAGIC CREATE OR REPLACE TABLE silver.transiciones
# MAGIC COMMENT 'Cambio de periodo de cada numero: sentido, dias sin linea y si es el mismo cliente'
# MAGIC AS
# MAGIC WITH con_anterior AS (
# MAGIC   SELECT *,
# MAGIC          LAG(tipo)    OVER w AS tipo_ant,
# MAGIC          LAG(sub_id)  OVER w AS sub_ant,
# MAGIC          LAG(baja_ts) OVER w AS baja_ant,
# MAGIC          LAG(cust_id) OVER w AS cust_ant
# MAGIC   FROM silver.periodos
# MAGIC   WHERE inicio_ts IS NOT NULL
# MAGIC   WINDOW w AS (PARTITION BY isdn9 ORDER BY inicio_ts, sub_id))
# MAGIC SELECT isdn9, sub_ant, sub_id, tipo_ant, tipo,
# MAGIC        concat(tipo_ant, '→', tipo)    AS transicion,
# MAGIC        baja_ant, inicio_ts, fuente_inicio,
# MAGIC        datediff(inicio_ts, baja_ant)  AS dias,
# MAGIC        CASE WHEN cust_ant IS NULL OR cust_id IS NULL THEN NULL ELSE cust_ant = cust_id END AS mismo_cliente,
# MAGIC        CASE WHEN baja_ant IS NULL                     THEN 'anterior sin baja'
# MAGIC             WHEN datediff(inicio_ts, baja_ant) < 0    THEN 'solape'
# MAGIC             WHEN datediff(inicio_ts, baja_ant) = 0    THEN '0'
# MAGIC             WHEN datediff(inicio_ts, baja_ant) <= 3   THEN '1-3'
# MAGIC             WHEN datediff(inicio_ts, baja_ant) <= 30  THEN '4-30'
# MAGIC             ELSE '>30' END AS rango_dias
# MAGIC FROM con_anterior
# MAGIC WHERE tipo_ant IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) resumen
# MAGIC SELECT transicion, mismo_cliente, rango_dias, count(*) AS n,
# MAGIC        round(100 * count(*) / sum(count(*)) OVER (PARTITION BY transicion), 1) AS pct_de_la_transicion
# MAGIC FROM silver.transiciones
# MAGIC GROUP BY transicion, mismo_cliente, rango_dias
# MAGIC ORDER BY transicion, mismo_cliente DESC, rango_dias;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.5 · Contrastar con los trámites de migración
# MAGIC Los trámites dicen **cuándo se pidió** la migración. Ojo: `CM_POS.MIGRATION_REQUESTS` **no** es migración PRE↔POS (`plan_current`, `plan_new`, `process_date`: es cambio de plan pospago). Las fuentes son:
# MAGIC - PRE→POS: `bronze.cm_pos__transfer_request` (`OLD_SUB_ID` prepago → `NEW_SUB_ID` pospago) y `bronze.cm_pre__request_transfer_postpaid` (`SUB_ID` → `SUB_ID_POS`);
# MAGIC - POS→PRE: `bronze.cm_pre__transfer_request`.
# MAGIC
# MAGIC **Tareas:**
# MAGIC 1. Mira las columnas de esas tres tablas: ¿cuál es el número, cuál la fecha, cuál el estado del trámite? Júntalas en `silver.tramites_migracion`.
# MAGIC 2. Une cada transición `PRE→POS` con su trámite por `isdn9` y calcula la diferencia en días entre el trámite y tu `inicio_ts`.
# MAGIC 3. Distribución: mismo día, 1–3 días, más de 3, **sin trámite**.
# MAGIC 4. Al revés: trámites aprobados que **no** tienen transición en SUB_MB.
# MAGIC 5. (Opcional) Cuarta fuente, los eventos del CRM: `bronze.cm_pos__action_log_trans_code` le pone a cada acción un `BUSINESS_CODE`. `PRE_2_POST` / `POST_2_PRE` son migraciones; `POS_2_POST` / `PRE_2_PRE` son cambios de plan. Agrégalos a `silver.tramites_migracion` y repite 2–3. Solo cubren la ventana de logs.
# MAGIC
# MAGIC **Terminado cuando:** puedes decir qué campo de qué tabla da la fecha real de la migración, con un porcentaje que lo respalde.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze.cm_pos__transfer_request

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze.cm_pre__transfer_request

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC -- E4.5 · 1) las tres fuentes de trámite PRE<->POS en una tabla
# MAGIC --   TRANSFER_REQUEST (CM_POS y CM_PRE): ID, OLD_SUB_ID, NEW_SUB_ID, ISDN, ID_TYPE, ID_NO, STATUS, REQUEST_DATETIME, ...
# MAGIC --   REQUEST_TRANSFER_POSTPAID: cola de pedidos PRE->POS; trae el SUB_ID prepago pero no el SUB_ID pospago
# MAGIC CREATE OR REPLACE TABLE silver.tramites_migracion
# MAGIC COMMENT 'Tramites de migracion PRE<->POS: fuente, sentido, subs, numero, fecha y estado'
# MAGIC AS
# MAGIC SELECT 'CM_POS.TRANSFER_REQUEST' AS fuente, 'PRE→POS' AS sentido,
# MAGIC        OLD_SUB_ID AS sub_origen, NEW_SUB_ID AS sub_destino,
# MAGIC        silver.f_isdn9(ISDN) AS isdn9, silver.f_ts(REQUEST_DATETIME) AS tramite_ts, STATUS AS estado
# MAGIC FROM bronze.cm_pos__transfer_request
# MAGIC UNION ALL
# MAGIC SELECT 'CM_PRE.REQUEST_TRANSFER_POSTPAID', 'PRE→POS',
# MAGIC        SUB_ID, CAST(NULL AS STRING),
# MAGIC        silver.f_isdn9(ISDN), silver.f_ts(CREATE_DATE), STATUS
# MAGIC FROM bronze.cm_pre__request_transfer_postpaid
# MAGIC UNION ALL
# MAGIC SELECT 'CM_PRE.TRANSFER_REQUEST', 'POS→PRE',
# MAGIC        OLD_SUB_ID, NEW_SUB_ID,
# MAGIC        silver.f_isdn9(ISDN), silver.f_ts(REQUEST_DATETIME), STATUS
# MAGIC FROM bronze.cm_pre__transfer_request;
# MAGIC
# MAGIC SELECT fuente, sentido, estado, count(*) AS tramites, min(tramite_ts) AS desde, max(tramite_ts) AS hasta
# MAGIC FROM silver.tramites_migracion
# MAGIC GROUP BY fuente, sentido, estado
# MAGIC ORDER BY fuente, tramites DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- comprobación del sentido: en qué SUB_MB existen el origen y el destino de cada fuente
# MAGIC SELECT r.fuente, count(*) AS tramites,
# MAGIC        count_if(o_pre.SUB_ID IS NOT NULL) AS origen_en_pre, count_if(o_pos.SUB_ID IS NOT NULL) AS origen_en_pos,
# MAGIC        count_if(d_pre.SUB_ID IS NOT NULL) AS destino_en_pre, count_if(d_pos.SUB_ID IS NOT NULL) AS destino_en_pos
# MAGIC FROM silver.tramites_migracion r
# MAGIC LEFT JOIN bronze.cm_pre__sub_mb o_pre ON o_pre.SUB_ID = r.sub_origen
# MAGIC LEFT JOIN bronze.cm_pos__sub_mb o_pos ON o_pos.SUB_ID = r.sub_origen
# MAGIC LEFT JOIN bronze.cm_pre__sub_mb d_pre ON d_pre.SUB_ID = r.sub_destino
# MAGIC LEFT JOIN bronze.cm_pos__sub_mb d_pos ON d_pos.SUB_ID = r.sub_destino
# MAGIC GROUP BY r.fuente
# MAGIC ORDER BY r.fuente;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) cada migración con su trámite más cercano (primero el que calza por SUB_ID, luego el más próximo en fecha)
# MAGIC CREATE OR REPLACE TEMP VIEW migracion_vs_tramite AS
# MAGIC WITH mig AS (
# MAGIC   SELECT * FROM silver.transiciones WHERE transicion IN ('PRE→POS', 'POS→PRE')),
# MAGIC cand AS (
# MAGIC   SELECT m.isdn9, m.sub_id, m.transicion, m.mismo_cliente, m.inicio_ts, m.fuente_inicio,
# MAGIC          r.fuente, r.estado, r.tramite_ts,
# MAGIC          r.sub_destino = m.sub_id                 AS por_sub_id,
# MAGIC          datediff(m.inicio_ts, r.tramite_ts)     AS dias_tramite_a_inicio,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY m.sub_id, m.transicion, r.fuente
# MAGIC                             ORDER BY (r.sub_destino = m.sub_id) DESC,
# MAGIC                                      abs(unix_timestamp(m.inicio_ts) - unix_timestamp(r.tramite_ts))) AS rn
# MAGIC   FROM mig m
# MAGIC   JOIN silver.tramites_migracion r
# MAGIC     ON r.isdn9 = m.isdn9 AND r.sentido = m.transicion
# MAGIC    AND (r.sub_destino = m.sub_id OR abs(datediff(m.inicio_ts, r.tramite_ts)) <= 30))
# MAGIC SELECT * FROM cand WHERE rn = 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) distribución, solo dentro del periodo que cubre cada fuente (antes de su primer trámite no hay con qué comparar)
# MAGIC WITH cobertura AS (
# MAGIC   SELECT fuente, sentido, min(tramite_ts) AS desde, max(tramite_ts) AS hasta
# MAGIC   FROM silver.tramites_migracion GROUP BY fuente, sentido),
# MAGIC base AS (
# MAGIC   SELECT c.fuente, m.transicion, m.sub_id, m.mismo_cliente
# MAGIC   FROM silver.transiciones m
# MAGIC   JOIN cobertura c ON c.sentido = m.transicion
# MAGIC    AND m.inicio_ts BETWEEN c.desde - INTERVAL 3 DAYS AND c.hasta + INTERVAL 3 DAYS),
# MAGIC clasif AS (
# MAGIC   SELECT b.fuente, b.transicion, b.mismo_cliente,
# MAGIC          CASE WHEN v.sub_id IS NULL                    THEN 'sin trámite'
# MAGIC               WHEN abs(v.dias_tramite_a_inicio) = 0    THEN 'mismo día'
# MAGIC               WHEN abs(v.dias_tramite_a_inicio) <= 3   THEN '1-3 días'
# MAGIC               ELSE '>3 días' END AS rango
# MAGIC   FROM base b
# MAGIC   LEFT JOIN migracion_vs_tramite v
# MAGIC     ON v.sub_id = b.sub_id AND v.transicion = b.transicion AND v.fuente = b.fuente)
# MAGIC SELECT fuente, transicion, mismo_cliente, rango, count(*) AS migraciones,
# MAGIC        round(100 * count(*) / sum(count(*)) OVER (PARTITION BY fuente, transicion, mismo_cliente), 1) AS pct
# MAGIC FROM clasif
# MAGIC GROUP BY fuente, transicion, mismo_cliente, rango
# MAGIC ORDER BY fuente, transicion, mismo_cliente DESC, rango;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿qué regla de inicio calza mejor con la fecha del trámite?
# MAGIC SELECT fuente, fuente_inicio, count(*) AS n,
# MAGIC        percentile_approx(dias_tramite_a_inicio, 0.5) AS mediana_dias,
# MAGIC        round(100 * count_if(abs(dias_tramite_a_inicio) <= 1) / count(*), 1) AS pct_a_1_dia
# MAGIC FROM migracion_vs_tramite
# MAGIC GROUP BY fuente, fuente_inicio
# MAGIC ORDER BY fuente, n DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4) trámites sin transición en SUB_MB, por estado (así ves qué estado significa "aprobado")
# MAGIC WITH emparejados AS (
# MAGIC   SELECT DISTINCT r.fuente, r.isdn9, r.tramite_ts
# MAGIC   FROM silver.tramites_migracion r
# MAGIC   JOIN silver.transiciones t
# MAGIC     ON t.isdn9 = r.isdn9 AND t.transicion = r.sentido
# MAGIC    AND (t.sub_id = r.sub_destino OR abs(datediff(t.inicio_ts, r.tramite_ts)) <= 30))
# MAGIC SELECT r.fuente, r.estado, count(*) AS tramites,
# MAGIC        count(e.isdn9) AS con_transicion,
# MAGIC        count(*) - count(e.isdn9) AS sin_transicion,
# MAGIC        round(100 * (count(*) - count(e.isdn9)) / count(*), 1) AS pct_sin_transicion
# MAGIC FROM silver.tramites_migracion r
# MAGIC LEFT JOIN emparejados e
# MAGIC   ON e.fuente = r.fuente AND e.isdn9 = r.isdn9 AND e.tramite_ts <=> r.tramite_ts
# MAGIC GROUP BY r.fuente, r.estado
# MAGIC ORDER BY r.fuente, tramites DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════════════════════
# MAGIC -- Eventos del CRM con BUSINESS_CODE (ACTION_AUDIT + ACTION_LOG_TRANS_CODE)
# MAGIC -- Va en el notebook 04, después de E4.5 (necesita silver.f_ts, silver.f_isdn9,
# MAGIC -- silver.periodos, silver.gemelos y silver.tramites_migracion).
# MAGIC -- Ojo: CM_POS.ACTION_LOG_TRANS_CODE está en LOGS de 00_config: solo trae DIAS_LOGS días.
# MAGIC -- Códigos (DTR atenciones 09-12/11/2026):
# MAGIC --   PRE_2_POST  -> migración PRE→POS (ACTION_CODE 00 individual, 666 back-office/masivo), PK_ID = sub pospago nuevo
# MAGIC --   POST_2_PRE  -> migración POS→PRE (ACTION_CODE 00), PK_ID = sub prepago nuevo
# MAGIC --   POS_2_POST (78, 198) y PRE_2_PRE (33) -> cambio de plan, NO migración
# MAGIC --   NEW_PRE / NEW_POST (00, 666) -> alta; MNP_NEW (00) -> portabilidad de entrada
# MAGIC -- ════════════════════════════════════════════════════════════════════════════
# MAGIC  
# MAGIC -- 1) Un trámite por fila: ACTION_AUDIT (pre y pos) + ACTION_LOG_TRANS_CODE, con el tipo de evento según BUSINESS_CODE
# MAGIC CREATE OR REPLACE TABLE silver.eventos_crm
# MAGIC CLUSTER BY (isdn9)
# MAGIC COMMENT 'ACTION_AUDIT pre/pos + CM_POS.ACTION_LOG_TRANS_CODE: un tramite por fila, tipo segun BUSINESS_CODE'
# MAGIC AS
# MAGIC WITH aa AS (
# MAGIC   SELECT 'PRE' AS tipo_linea, ACTION_AUDIT_ID, PK_ID, PK_TYPE, ACTION_CODE, USER_NAME, SHOP_CODE, DESCRIPTION, ISSUE_DATETIME
# MAGIC   FROM bronze.cm_pre__action_audit
# MAGIC   UNION ALL
# MAGIC   SELECT 'POS', ACTION_AUDIT_ID, PK_ID, PK_TYPE, ACTION_CODE, USER_NAME, SHOP_CODE, DESCRIPTION, ISSUE_DATETIME
# MAGIC   FROM bronze.cm_pos__action_audit)
# MAGIC SELECT aa.tipo_linea,
# MAGIC        aa.PK_ID                          AS sub_id,
# MAGIC        silver.f_isdn9(t.OBJECT_NUMBER)   AS isdn9,          -- en CHANGE_ISDN es el número ANTERIOR
# MAGIC        CASE WHEN t.BUSINESS_CODE = 'CHANGE_ISDN'
# MAGIC             THEN silver.f_isdn9(regexp_extract(aa.DESCRIPTION, 'NEW ISDN = ([0-9]+)', 1)) END AS isdn9_nuevo,
# MAGIC        t.BUSINESS_CODE                   AS business_code,
# MAGIC        aa.ACTION_CODE                    AS action_code,
# MAGIC        CASE t.BUSINESS_CODE
# MAGIC             WHEN 'PRE_2_POST'         THEN 'MIGRACION'
# MAGIC             WHEN 'POST_2_PRE'         THEN 'MIGRACION'
# MAGIC             WHEN 'POS_2_POST'         THEN 'CAMBIO_PLAN'
# MAGIC             WHEN 'PRE_2_PRE'          THEN 'CAMBIO_PLAN'
# MAGIC             WHEN 'NEW_PRE'            THEN 'ALTA'
# MAGIC             WHEN 'NEW_POST'           THEN 'ALTA'
# MAGIC             WHEN 'MNP_NEW'            THEN 'PORTABILIDAD_IN'
# MAGIC             WHEN 'DELETE_SUB'         THEN 'SOLICITUD_BAJA'
# MAGIC             WHEN 'TERMINATE'          THEN 'SOLICITUD_BAJA'
# MAGIC             WHEN 'CHANGE_OWNER'       THEN 'CAMBIO_TITULAR'
# MAGIC             WHEN 'CHANGE_ISDN'        THEN 'CAMBIO_NUMERO'
# MAGIC             WHEN 'CHANGE_SIM'         THEN 'CAMBIO_SIM'
# MAGIC             WHEN 'BLOCK_IMEI_ISDN'    THEN 'BLOQUEO_ROBO_PERDIDA'
# MAGIC             WHEN 'LINE_SUSPENSION'    THEN 'SUSPENSION'
# MAGIC             WHEN 'OPEN_LINE_CUST_REQ' THEN 'REACTIVACION'
# MAGIC             ELSE 'OTRO' END              AS tipo_evento,
# MAGIC        CASE t.BUSINESS_CODE WHEN 'PRE_2_POST' THEN 'PRE→POS' WHEN 'POST_2_PRE' THEN 'POS→PRE' END AS sentido,
# MAGIC        silver.f_ts(aa.ISSUE_DATETIME)    AS fecha_ts,
# MAGIC        silver.f_ts(t.INSERT_TIME)        AS insert_ts,
# MAGIC        aa.USER_NAME AS user_name, aa.SHOP_CODE AS shop_code, aa.DESCRIPTION AS descripcion,
# MAGIC        aa.ACTION_AUDIT_ID AS action_audit_id, t.TRANS_CODE AS trans_code
# MAGIC FROM bronze.cm_pos__action_log_trans_code t
# MAGIC JOIN aa ON aa.ACTION_AUDIT_ID = t.ACTION_AUDIT_ID
# MAGIC WHERE aa.PK_TYPE = '3';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) controles: el ID de ACTION_AUDIT no debe calzar en pre y pos a la vez; BUSINESS_CODE coherente con el tipo de línea
# MAGIC SELECT count(*) AS trans_codes_en_pre_y_pos
# MAGIC FROM (SELECT trans_code FROM silver.eventos_crm GROUP BY trans_code HAVING count(DISTINCT tipo_linea) > 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT business_code, action_code, tipo_linea, count(*) AS n, min(fecha_ts) AS desde, max(fecha_ts) AS hasta
# MAGIC FROM silver.eventos_crm GROUP BY business_code, action_code, tipo_linea ORDER BY business_code, n DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) la prueba: cada migración del CRM contra lo que dicen SUB_MB y las tablas de trámite
# MAGIC WITH mig AS (
# MAGIC   SELECT tipo_linea, sub_id, sentido, action_code, min(fecha_ts) AS evento_ts
# MAGIC   FROM silver.eventos_crm WHERE tipo_evento = 'MIGRACION'
# MAGIC   GROUP BY tipo_linea, sub_id, sentido, action_code),
# MAGIC gem AS (
# MAGIC   SELECT 'POS' AS tipo, sub_pos AS sub_id FROM silver.gemelos
# MAGIC   UNION SELECT 'PRE', sub_pre FROM silver.gemelos),
# MAGIC tr AS (SELECT DISTINCT fuente, sub_destino FROM silver.tramites_migracion WHERE sub_destino IS NOT NULL)
# MAGIC SELECT m.sentido, m.action_code,
# MAGIC        count(*)                                                            AS migraciones,
# MAGIC        count(p.sub_id)                                                     AS en_sub_mb,
# MAGIC        count_if(p.mig_ts IS NOT NULL)                                      AS con_mig_datetime,
# MAGIC        count_if(abs(timestampdiff(MINUTE, p.mig_ts,  m.evento_ts)) <= 15)  AS mig_a_15_min,
# MAGIC        count_if(abs(timestampdiff(MINUTE, p.alta_ts, m.evento_ts)) <= 15)  AS sta_a_15_min,
# MAGIC        count_if(abs(timestampdiff(MINUTE, p.inicio_ts, m.evento_ts)) <= 15) AS inicio_ts_a_15_min,
# MAGIC        count(g.sub_id)                                                     AS con_gemelo,
# MAGIC        count_if(t1.sub_destino IS NOT NULL)                                AS en_transfer_request,
# MAGIC        count_if(t2.sub_destino IS NOT NULL)                                AS en_request_postpaid,
# MAGIC        count_if(t3.sub_destino IS NOT NULL)                                AS en_pre_transfer_request
# MAGIC FROM mig m
# MAGIC LEFT JOIN silver.periodos p ON p.tipo = m.tipo_linea AND p.sub_id = m.sub_id
# MAGIC LEFT JOIN gem g ON g.tipo = m.tipo_linea AND g.sub_id = m.sub_id
# MAGIC LEFT JOIN tr t1 ON t1.fuente = 'CM_POS.TRANSFER_REQUEST'          AND t1.sub_destino = m.sub_id
# MAGIC LEFT JOIN tr t2 ON t2.fuente = 'CM_PRE.REQUEST_TRANSFER_POSTPAID' AND t2.sub_destino = m.sub_id
# MAGIC LEFT JOIN tr t3 ON t3.fuente = 'CM_PRE.TRANSFER_REQUEST'          AND t3.sub_destino = m.sub_id
# MAGIC GROUP BY m.sentido, m.action_code
# MAGIC ORDER BY m.sentido, m.action_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4) altas pospago duplicadas (00 + 666 del mismo SUB_ID)
# MAGIC SELECT count(*) AS subs_con_00_y_666
# MAGIC FROM (SELECT sub_id FROM silver.eventos_crm WHERE business_code = 'NEW_POST'
# MAGIC       GROUP BY sub_id HAVING count(DISTINCT action_code) > 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- E4.5: el CRM como cuarta fuente de trámite (solo cubre la ventana de logs)
# MAGIC INSERT INTO silver.tramites_migracion
# MAGIC SELECT 'ACTION_LOG_TRANS_CODE' AS fuente, sentido, CAST(NULL AS STRING) AS sub_origen, sub_id AS sub_destino,
# MAGIC        isdn9, min(fecha_ts) AS tramite_ts, concat(business_code, ' ', action_code) AS estado
# MAGIC FROM silver.eventos_crm
# MAGIC WHERE tipo_evento = 'MIGRACION'
# MAGIC GROUP BY sentido, sub_id, isdn9, business_code, action_code;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.6 · Validar con ACTION_AUDIT (solo en la ventana de logs)
# MAGIC Los logs cubren solo los últimos `DIAS_LOGS` días. Dentro de esa ventana:
# MAGIC 1. Toma las líneas (PRE y POS) con `alta_ts` en la ventana **más** las que tienen su hito `'00'` en la ventana. Si solo tomas la alta, las migradas no aparecen: su STA es viejo, heredado.
# MAGIC 2. Busca su hito ACTION_CODE `'00'` en ACTION_AUDIT (por `PK_ID` = `sub_id`; revisa en `PK_TYPE` cuál es el tipo de una línea).
# MAGIC 3. Calcula la diferencia en minutos. Regla: ≤ 15 min es normal.
# MAGIC 4. Compara: líneas **con** gemelo (STA heredado) vs **sin** gemelo; y en pospago, las que entraron por `'666'` vs el resto. ¿En cuál falla más la regla de los 15 minutos?
# MAGIC
# MAGIC Ojo: en las altas por `'666'` (back-office o masivas) el `'00'` llega después, y las de STATUS 9 todavía no lo tienen. Que fallen ahí es esperable.
# MAGIC
# MAGIC **Terminado cuando:** tienes el % que cumple la regla en cada grupo. Las migradas deberían fallar contra el STA y cumplir contra `MIG_DATETIME`: su alta real es otra.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC -- 1) qué PK_TYPE apunta a una línea: el que tiene casi todos sus PK_ID en SUB_MB.SUB_ID
# MAGIC SELECT 'PRE' AS tipo, a.PK_TYPE, count(*) AS eventos, count(s.SUB_ID) AS pk_es_sub_id,
# MAGIC        round(100 * count(s.SUB_ID) / count(*), 1) AS pct
# MAGIC FROM bronze.cm_pre__action_audit a
# MAGIC LEFT JOIN (SELECT DISTINCT SUB_ID FROM bronze.cm_pre__sub_mb) s ON s.SUB_ID = a.PK_ID
# MAGIC GROUP BY a.PK_TYPE
# MAGIC UNION ALL
# MAGIC SELECT 'POS', a.PK_TYPE, count(*), count(s.SUB_ID), round(100 * count(s.SUB_ID) / count(*), 1)
# MAGIC FROM bronze.cm_pos__action_audit a
# MAGIC LEFT JOIN (SELECT DISTINCT SUB_ID FROM bronze.cm_pos__sub_mb) s ON s.SUB_ID = a.PK_ID
# MAGIC GROUP BY a.PK_TYPE
# MAGIC ORDER BY tipo, eventos DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW hitos_alta AS
# MAGIC SELECT 'PRE' AS tipo, PK_ID AS sub_id, silver.f_ts(ISSUE_DATETIME) AS audit_ts
# MAGIC FROM bronze.cm_pre__action_audit WHERE ACTION_CODE = '00' AND PK_TYPE = '3'
# MAGIC UNION ALL
# MAGIC SELECT 'POS', PK_ID, silver.f_ts(ISSUE_DATETIME)
# MAGIC FROM bronze.cm_pos__action_audit WHERE ACTION_CODE = '00' AND PK_TYPE = '3';
# MAGIC  
# MAGIC WITH ventana AS (
# MAGIC   SELECT tipo, min(audit_ts) AS desde, max(audit_ts) AS hasta FROM hitos_alta GROUP BY tipo),
# MAGIC universo AS (
# MAGIC   -- líneas con alta en la ventana + líneas con hito '00' en la ventana
# MAGIC   -- (las migradas tienen STA viejo heredado: si solo filtras por alta, no aparecen)
# MAGIC   SELECT p.tipo, p.sub_id
# MAGIC   FROM silver.periodos p JOIN ventana v ON v.tipo = p.tipo AND p.alta_ts BETWEEN v.desde AND v.hasta
# MAGIC   UNION
# MAGIC   SELECT tipo, sub_id FROM hitos_alta),
# MAGIC con_gemelo AS (
# MAGIC   SELECT 'PRE' AS tipo, sub_pre AS sub_id FROM silver.gemelos
# MAGIC   UNION
# MAGIC   SELECT 'POS', sub_pos FROM silver.gemelos),
# MAGIC recibidas AS (
# MAGIC   -- pospago: altas que entran por 666 "Receive active subscriber" (back-office o masivo); su '00' llega después
# MAGIC   SELECT DISTINCT PK_ID AS sub_id FROM bronze.cm_pos__action_audit WHERE ACTION_CODE = '666' AND PK_TYPE = '3'),
# MAGIC lineas AS (
# MAGIC   SELECT p.tipo, p.sub_id, p.alta_ts, p.mig_ts, cg.sub_id IS NOT NULL AS tiene_gemelo,
# MAGIC          p.tipo = 'POS' AND r.sub_id IS NOT NULL AS via_666
# MAGIC   FROM universo u
# MAGIC   JOIN silver.periodos p ON p.tipo = u.tipo AND p.sub_id = u.sub_id
# MAGIC   LEFT JOIN con_gemelo cg ON cg.tipo = p.tipo AND cg.sub_id = p.sub_id
# MAGIC   LEFT JOIN recibidas r ON r.sub_id = p.sub_id),
# MAGIC hito AS (
# MAGIC   SELECT l.tipo, l.sub_id, l.tiene_gemelo, l.via_666, l.alta_ts, l.mig_ts,
# MAGIC          min_by(h.audit_ts, abs(unix_timestamp(h.audit_ts) - unix_timestamp(l.alta_ts))) AS audit_ts
# MAGIC   FROM lineas l
# MAGIC   LEFT JOIN hitos_alta h ON h.tipo = l.tipo AND h.sub_id = l.sub_id
# MAGIC   GROUP BY l.tipo, l.sub_id, l.tiene_gemelo, l.via_666, l.alta_ts, l.mig_ts)
# MAGIC SELECT tipo, tiene_gemelo, via_666,
# MAGIC        count(*)                                                        AS lineas,
# MAGIC        count_if(audit_ts IS NULL)                                      AS sin_hito_00,
# MAGIC        count_if(abs(timestampdiff(MINUTE, alta_ts, audit_ts)) <= 15)   AS cumple_con_sta,
# MAGIC        round(100 * count_if(abs(timestampdiff(MINUTE, alta_ts, audit_ts)) <= 15) / count(audit_ts), 1)
# MAGIC                                                                        AS pct_cumple_con_sta,
# MAGIC        count_if(abs(timestampdiff(MINUTE, mig_ts, audit_ts)) <= 15)    AS cumple_con_mig,
# MAGIC        percentile_approx(abs(timestampdiff(MINUTE, alta_ts, audit_ts)), 0.5) AS mediana_min_sta
# MAGIC FROM hito
# MAGIC GROUP BY tipo, tiene_gemelo, via_666
# MAGIC ORDER BY tipo, tiene_gemelo, via_666;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.7 · Titularidad
# MAGIC **Tareas:**
# MAGIC 1. `DESCRIBE bronze.cm_pos__change_owner_log`: ¿qué columnas son el titular anterior, el nuevo, la línea y la fecha? `DB_TYPE` dice de qué origen es el cambio (en BI_REPORT, 0 = pospago y 1 = prepago).
# MAGIC 2. Para cada número, el **último** cambio (ventana `ROW_NUMBER() ... ORDER BY fecha DESC`, con un desempate). Hazlo por `isdn9` y no por SUB_ID: en pospago un cambio de titular puede crear otro SUB_ID.
# MAGIC 3. Compara el titular nuevo de ese último cambio con el `cust_id` actual de `silver.lineas`. ¿Cuántas coinciden?
# MAGIC 4. Mira 20 que no coinciden: ¿hubo una migración **después** del cambio? ¿El cambio quedó pendiente o rechazado?
# MAGIC
# MAGIC **Terminado cuando:** tienes el % de coincidencia y una explicación para los casos más frecuentes que no coinciden.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze.cm_pos__change_owner_log

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC -- 1) explorar: qué cambia en cada registro
# MAGIC SELECT DB_TYPE, STATUS, count(*) AS cambios,
# MAGIC        count_if(TO_SUB_ID = FROM_SUB_ID)   AS mismo_sub,
# MAGIC        count_if(TO_CUST_ID = FROM_CUST_ID) AS mismo_cliente,
# MAGIC        min(CREATE_TIME) AS desde, max(CREATE_TIME) AS hasta
# MAGIC FROM bronze.cm_pos__change_owner_log
# MAGIC GROUP BY DB_TYPE, STATUS
# MAGIC ORDER BY cambios DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) y 3) último cambio de cada número vs titular actual
# MAGIC CREATE OR REPLACE TEMP VIEW titularidad AS
# MAGIC WITH cambios AS (
# MAGIC   SELECT silver.f_isdn9(ISDN) AS isdn9, coalesce(TO_SUB_ID, FROM_SUB_ID) AS sub_id,
# MAGIC          FROM_CUST_ID, TO_CUST_ID, STATUS AS estado_cambio, DB_TYPE,
# MAGIC          silver.f_ts(CREATE_TIME) AS cambio_ts, ID
# MAGIC   FROM bronze.cm_pos__change_owner_log),
# MAGIC ultimo AS (
# MAGIC   SELECT * FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY isdn9 ORDER BY cambio_ts DESC, ID DESC) AS rn
# MAGIC     FROM cambios WHERE isdn9 IS NOT NULL)
# MAGIC   WHERE rn = 1),
# MAGIC ult_transicion AS (
# MAGIC   SELECT isdn9, max(inicio_ts) AS ultima_transicion_ts FROM silver.transiciones GROUP BY isdn9)
# MAGIC SELECT u.*, l.tipo, l.cust_id AS cust_actual, l.status AS status_linea, l.baja_ts,
# MAGIC        t.ultima_transicion_ts,
# MAGIC        CASE WHEN l.sub_id IS NULL          THEN 'linea no encontrada'
# MAGIC             WHEN l.cust_id = u.TO_CUST_ID   THEN 'coincide'
# MAGIC             WHEN l.cust_id = u.FROM_CUST_ID THEN 'sigue con el titular anterior'
# MAGIC             ELSE 'otro cliente' END AS resultado,
# MAGIC        coalesce(t.ultima_transicion_ts > u.cambio_ts, false) AS hubo_transicion_despues
# MAGIC FROM ultimo u
# MAGIC LEFT JOIN silver.lineas l ON l.sub_id = u.sub_id AND l.isdn9 = u.isdn9
# MAGIC LEFT JOIN ult_transicion t ON t.isdn9 = u.isdn9;
# MAGIC
# MAGIC SELECT resultado, estado_cambio, hubo_transicion_despues, count(*) AS n,
# MAGIC        round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct
# MAGIC FROM titularidad
# MAGIC GROUP BY resultado, estado_cambio, hubo_transicion_despues
# MAGIC ORDER BY n DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4) 20 que no coinciden
# MAGIC SELECT * FROM titularidad WHERE resultado <> 'coincide' ORDER BY cambio_ts DESC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.8 · Funciones de orden superior: la historia de un número en una fila
# MAGIC **Tareas:**
# MAGIC 1. Por `isdn9`, junta sus periodos en un arreglo ordenado: `array_sort(collect_list(struct(inicio_ts, tipo, sub_id, status)))`.
# MAGIC 2. Con `transform`, arma un texto legible tipo `PRE(2019-03) → POS(2023-07) → PRE(2025-01)`.
# MAGIC 3. Con `filter` y `size`, cuenta cuántos periodos POS tiene cada número; con `exists`, marca los que alguna vez fueron pospago; con `aggregate`, cuenta los periodos **cerrados** (STATUS 0, 3 o 4, no solo el 3).
# MAGIC 4. Muestra los 20 números con más periodos.
# MAGIC 5. Haz el camino inverso con `explode` y comprueba que vuelves al mismo número de filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC -- 1) la historia de cada número en una fila
# MAGIC CREATE OR REPLACE TEMP VIEW historia AS
# MAGIC SELECT isdn9,
# MAGIC        array_sort(collect_list(struct(inicio_ts, tipo, sub_id, status))) AS periodos
# MAGIC FROM silver.periodos
# MAGIC WHERE inicio_ts IS NOT NULL
# MAGIC GROUP BY isdn9;
# MAGIC  
# MAGIC -- 2) 3) 4) transform, filter, exists
# MAGIC SELECT isdn9,
# MAGIC        size(periodos) AS n_periodos,
# MAGIC        array_join(transform(periodos, p -> concat(p.tipo, '(', date_format(p.inicio_ts, 'yyyy-MM'), ')')),
# MAGIC                   ' → ') AS historia,
# MAGIC        size(filter(periodos, p -> p.tipo = 'POS')) AS periodos_pos,
# MAGIC        exists(periodos, p -> p.tipo = 'POS')        AS alguna_vez_pospago,
# MAGIC        aggregate(periodos, 0, (acc, p) -> acc + IF(p.status IN ('0', '3', '4'), 1, 0)) AS periodos_cerrados   -- 0 y 4 también cierran
# MAGIC FROM historia
# MAGIC ORDER BY n_periodos DESC, isdn9
# MAGIC LIMIT 20;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5) el camino inverso
# MAGIC SELECT (SELECT count(*) FROM historia LATERAL VIEW explode(periodos) e AS p)  AS filas_explode,
# MAGIC        (SELECT count(*) FROM silver.periodos WHERE inicio_ts IS NOT NULL)      AS filas_origen;
# MAGIC  
# MAGIC SELECT h.isdn9, p.inicio_ts, p.tipo, p.sub_id, p.status
# MAGIC FROM historia h LATERAL VIEW explode(h.periodos) e AS p
# MAGIC ORDER BY h.isdn9, p.inicio_ts
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.9 · Pivot: STATUS × ACT_STATUS
# MAGIC **Tarea:** una tabla por `tipo` con STATUS en filas, ACT_STATUS en columnas (los 6 más frecuentes) y conteos.
# MAGIC - Prepago usa 2 dígitos y pospago 3: pivotea por los 2 primeros (`substr(act_status, 1, 2)`), como hace BI_REPORT para juntar los dos orígenes.
# MAGIC - Decodifica STATUS con `gold.catalogo_valores` y ACT_STATUS con `silver.f_bloqueo` (E3.6). `silver.v_dominio` no sirve aquí: STATUS no está en AP_DOMAIN.
# MAGIC
# MAGIC **Pregunta:** ¿hay líneas cerradas (STATUS 0, 3 o 4) con bloqueo? ¿Qué significa? (Pista: ACT_STATUS no se limpia al cerrar la línea.)

# COMMAND ----------

# E4.9 · los 6 ACT_STATUS más frecuentes salen de los datos (PIVOT necesita la lista escrita)
# Prepago usa 2 dígitos y pospago 3: para pivotear juntos se comparan los 2 primeros (lo hace BI_REPORT)
top6 = [r.a2 for r in spark.sql("""
    SELECT substr(act_status, 1, 2) AS a2, count(*) AS n FROM silver.lineas
    WHERE act_status IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT 6""").collect()]
lista = ", ".join(f"'{v}'" for v in top6)
print("columnas:", lista)
 
pv = spark.sql(f"""
  SELECT * FROM (SELECT tipo, status, substr(act_status, 1, 2) AS a2 FROM silver.lineas)
  PIVOT (count(1) FOR a2 IN ({lista}))
  ORDER BY tipo, status""")
 
# decodificar STATUS con gold.catalogo_valores de E3.6 (v_dominio no sirve: STATUS no está en AP_DOMAIN)
nombres = spark.sql("""
  SELECT CASE tabla WHEN 'cm_pre__sub_mb' THEN 'PRE' ELSE 'POS' END AS tipo, valor AS status, significado
  FROM gold.catalogo_valores WHERE columna = 'STATUS'""")
display(nombres.join(pv, ["tipo", "status"], "right").orderBy("tipo", "status"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- La misma tabla en SQL puro (forma de examen): la lista IN se escribe a mano
# MAGIC SELECT * FROM (SELECT tipo, status, substr(act_status, 1, 2) AS a2 FROM silver.lineas)
# MAGIC PIVOT (count(1) FOR a2 IN ('00' AS sin_bloqueo, '01' AS deuda_1v, '02' AS deuda_2v,
# MAGIC                            '10' AS cliente_1v, '20' AS cliente_2v, '03' AS pre_kit))
# MAGIC ORDER BY tipo, status;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- la pregunta: líneas cerradas que conservan un bloqueo (es el último bloqueo que tenían al cerrarse)
# MAGIC SELECT tipo, silver.f_estado_linea(tipo, status, act_status) AS estado_final,
# MAGIC        silver.f_bloqueo(tipo, act_status) AS bloqueo, count(*) AS lineas
# MAGIC FROM silver.lineas
# MAGIC WHERE status IN ('0', '3', '4') AND act_status RLIKE '[12]'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY lineas DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.10 · UDF vs función nativa
# MAGIC **Tarea:** escribe una UDF de Python que normalice el número igual que `isdn9` y compara el tiempo de las dos sobre `bronze.cm_pre__sub_mb`.
# MAGIC "Igual que `isdn9`" quiere decir: recorta a 9 dígitos solo si el valor es un número en claro, y deja como está un número seudonimizado. Si la UDF le quita las letras a cualquier texto, ya no hace lo mismo.
# MAGIC Mira el Query Profile: ¿qué cambia cuando hay una UDF de Python?
# MAGIC
# MAGIC Caso real del mismo problema, en Oracle: `F_BLOCK_STATUS` de BI_REPORT hace un `SELECT … FROM dual` por fila, y un `INSERT` que filtraba con ella afectó a DB REPORT. `F_BLOCK_STATUS(x) IN ('B1W','Active')` equivale a `x IS NULL OR (x NOT LIKE '%3%' AND x NOT LIKE '%2%')`, sin llamar a la función.

# COMMAND ----------

import re, time
from pyspark.sql.types import StringType
 
PATRON = re.compile(r"^[0-9 +-]+$")
 
@F.udf(StringType())
def isdn9_udf(x):
    """Igual que isdn9() del config: solo recorta si el valor es un número en claro."""
    if x is None:
        return None
    if PATRON.match(x):
        return re.sub(r"[^0-9]", "", x)[-9:]
    return x                      # seudonimizado: queda igual
 
df = spark.table(tb("CM_PRE.SUB_MB"))
c = F.col(COL['isdn'])
 
def medir(expresion, etiqueta):
    t0 = time.time()
    n = df.select(expresion.alias("k")).where("k IS NOT NULL").count()   # el filtro obliga a calcular k
    print(f"{etiqueta:<7} {n:,} filas  {time.time() - t0:,.1f} s")
 
for _ in range(2):                # la segunda vuelta ya no paga la lectura en frío
    medir(isdn9_udf(c), "UDF")
    medir(F.expr(isdn9(COL['isdn'])), "nativa")
 
# ¿dan lo mismo? (debe ser 0)
print(df.where(~isdn9_udf(c).eqNullSafe(F.expr(isdn9(COL['isdn'])))).count(), "diferencias")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── 2 · ¿STATUS 0 sin fecha de baja = línea cedida en un cambio de titular? ──────────
# MAGIC -- Pistas: POS 0 sin baja 295.820 ≈ POS→POS otro cliente "anterior sin baja" 287.332 ≈ CHANGE_OWNER_LOG DB_TYPE 0 282.465
# MAGIC --         PRE 0 sin baja 184.570 ≈ PRE→PRE otro cliente "anterior sin baja" 146.696;  DB_TYPE 1 219.960
# MAGIC -- Si pct sale ~90 % o más en status 0 / sin_baja = true, la etiqueta de f_estado_linea para ese caso
# MAGIC -- es "CERRADA por cambio de titular" y no "paso a prepago / pospago".
# MAGIC WITH cedidas AS (
# MAGIC   SELECT CASE DB_TYPE WHEN '0' THEN 'POS' WHEN '1' THEN 'PRE' END AS tipo, FROM_SUB_ID AS sub_id
# MAGIC   FROM bronze.cm_pos__change_owner_log
# MAGIC   GROUP BY 1, 2)
# MAGIC SELECT l.tipo, l.status,
# MAGIC        l.baja_ts IS NULL                                 AS sin_baja,
# MAGIC        l.tipo = 'PRE' AND l.act_status LIKE '%3%'        AS kit,
# MAGIC        count(*)                                          AS lineas,
# MAGIC        count_if(c.sub_id IS NOT NULL)                    AS cedidas_en_cambio_de_titular,
# MAGIC        round(100 * count_if(c.sub_id IS NOT NULL) / count(*), 1) AS pct
# MAGIC FROM silver.lineas l
# MAGIC LEFT JOIN cedidas c ON c.tipo = l.tipo AND c.sub_id = l.sub_id
# MAGIC WHERE l.status IN ('0', '3', '4')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY l.tipo, l.status, sin_baja DESC, kit;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── 3 · ¿Qué guarda NEW_SUB_ID en CM_POS.TRANSFER_REQUEST? ─────────────────────────
# MAGIC -- Solo 976.099 de 5,16 M NEW_SUB_ID existen en POS (y 1,6 M "existen" en PRE).
# MAGIC -- Por eso el chequeo CRM de E4.5 (por sub_destino) da 423 / 442, aunque el cruce por número y día da 89,8 %.
# MAGIC -- Si es_sub_pos_mismo_numero es bajo en los años recientes: el destino se cruza por isdn9 + fecha, no por NEW_SUB_ID.
# MAGIC SELECT year(t.tramite_ts)                                              AS anio,
# MAGIC        count(*)                                                        AS tramites,
# MAGIC        count_if(t.sub_destino IS NULL OR trim(t.sub_destino) IN ('', '0')) AS sin_new_sub_id,
# MAGIC        count_if(t.sub_destino = t.sub_origen)                          AS igual_al_origen,
# MAGIC        count_if(pos.sub_id IS NOT NULL)                                AS es_sub_pos,
# MAGIC        count_if(pos.isdn9 = t.isdn9)                                   AS es_sub_pos_mismo_numero,
# MAGIC        count_if(con.contract_id IS NOT NULL)                           AS es_contrato_pos,
# MAGIC        count_if(pre.isdn9 = t.isdn9)                                   AS es_sub_pre_mismo_numero
# MAGIC FROM silver.tramites_migracion t
# MAGIC LEFT JOIN silver.lineas pos ON pos.tipo = 'POS' AND pos.sub_id = t.sub_destino
# MAGIC LEFT JOIN silver.lineas pre ON pre.tipo = 'PRE' AND pre.sub_id = t.sub_destino
# MAGIC LEFT JOIN (SELECT DISTINCT contract_id FROM silver.lineas WHERE tipo = 'POS') con
# MAGIC        ON con.contract_id = t.sub_destino
# MAGIC WHERE t.fuente = 'CM_POS.TRANSFER_REQUEST'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY anio;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── 4 · Pospago nuevo (sin gemelo, sin 666): solo 37,2 % tiene el 00 a ±15 min del STA ────
# MAGIC -- Hipótesis: portabilidad (MNP_NEW). El STA es la venta y el 00 llega cuando se ejecuta la portabilidad.
# MAGIC -- En PRE la portabilidad es ~5 % de las altas (15.283 de 334.422 en el CRM) y la regla cumple 96,1 %.
# MAGIC CREATE OR REPLACE TEMP VIEW pos_nuevas AS
# MAGIC WITH alta_00 AS (
# MAGIC   SELECT PK_ID AS sub_id, min(silver.f_ts(ISSUE_DATETIME)) AS audit_ts
# MAGIC   FROM bronze.cm_pos__action_audit
# MAGIC   WHERE ACTION_CODE = '00' AND PK_TYPE = '3'
# MAGIC   GROUP BY PK_ID),
# MAGIC recibidas AS (
# MAGIC   SELECT DISTINCT PK_ID AS sub_id FROM bronze.cm_pos__action_audit WHERE ACTION_CODE = '666' AND PK_TYPE = '3'),
# MAGIC crm AS (
# MAGIC   SELECT sub_id, max(business_code) AS business_code
# MAGIC   FROM silver.eventos_crm
# MAGIC   WHERE tipo_linea = 'POS' AND action_code = '00'
# MAGIC   GROUP BY sub_id)
# MAGIC SELECT p.sub_id, p.alta_ts, a.audit_ts,
# MAGIC        timestampdiff(MINUTE, p.alta_ts, a.audit_ts)           AS min_00,
# MAGIC        coalesce(crm.business_code, '(fuera de la ventana del CRM)') AS business_code
# MAGIC FROM silver.periodos p
# MAGIC JOIN alta_00 a ON a.sub_id = p.sub_id
# MAGIC LEFT ANTI JOIN recibidas r      ON r.sub_id = p.sub_id
# MAGIC LEFT ANTI JOIN silver.gemelos g ON g.sub_pos = p.sub_id
# MAGIC LEFT JOIN crm                   ON crm.sub_id = p.sub_id
# MAGIC WHERE p.tipo = 'POS';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a) cuánto tarda el 00, por tipo de alta
# MAGIC SELECT business_code,
# MAGIC        CASE WHEN min_00 < -15    THEN '1 · 00 antes del STA'
# MAGIC             WHEN min_00 <= 15    THEN '2 · ±15 min'
# MAGIC             WHEN min_00 <= 60    THEN '3 · 15-60 min'
# MAGIC             WHEN min_00 <= 1440  THEN '4 · 1-24 h'
# MAGIC             WHEN min_00 <= 10080 THEN '5 · 1-7 días'
# MAGIC             ELSE '6 · más de 7 días' END                      AS demora_00,
# MAGIC        count(*)                                               AS lineas,
# MAGIC        round(100 * count(*) / sum(count(*)) OVER (PARTITION BY business_code), 1) AS pct_del_tipo
# MAGIC FROM pos_nuevas
# MAGIC GROUP BY ALL
# MAGIC ORDER BY business_code, demora_00;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) en las que no cumplen: qué acción sí ocurre a ±15 min del STA
# MAGIC SELECT a.ACTION_CODE, c.nombre, count(DISTINCT n.sub_id) AS lineas
# MAGIC FROM pos_nuevas n
# MAGIC JOIN bronze.cm_pos__action_audit a
# MAGIC   ON a.PK_ID = n.sub_id AND a.PK_TYPE = '3'
# MAGIC  AND abs(timestampdiff(MINUTE, n.alta_ts, silver.f_ts(a.ISSUE_DATETIME))) <= 15
# MAGIC LEFT JOIN silver.v_catalogo_acciones c ON c.origen = 'POS' AND c.action_code = a.ACTION_CODE
# MAGIC WHERE abs(n.min_00) > 15
# MAGIC GROUP BY ALL
# MAGIC ORDER BY lineas DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── 5 · Números con cientos de periodos (E4.8) ──────────────────────────────────
# MAGIC -- 929340007 tiene 2.727 periodos POS que empiezan en 2025-10, todos cerrados menos uno.
# MAGIC -- a) cuántos números son así y cuánto pesan
# MAGIC WITH n AS (
# MAGIC   SELECT isdn9, count(*) AS periodos
# MAGIC   FROM silver.periodos
# MAGIC   WHERE inicio_ts IS NOT NULL
# MAGIC   GROUP BY isdn9)
# MAGIC SELECT CASE WHEN periodos <= 5   THEN '1 · 1-5'
# MAGIC             WHEN periodos <= 20  THEN '2 · 6-20'
# MAGIC             WHEN periodos <= 100 THEN '3 · 21-100'
# MAGIC             ELSE '4 · más de 100' END        AS periodos_por_numero,
# MAGIC        count(*)                              AS numeros,
# MAGIC        sum(periodos)                         AS periodos,
# MAGIC        round(100 * sum(periodos) / sum(sum(periodos)) OVER (), 2) AS pct_periodos
# MAGIC FROM n
# MAGIC GROUP BY ALL
# MAGIC ORDER BY periodos_por_numero;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- b) los cinco peores: ¿un cliente, un contrato, muchas altas el mismo día? ¿cuánto vive cada línea?
# MAGIC SELECT isdn9, status, act_status,
# MAGIC        count(*)                                   AS periodos,
# MAGIC        count(DISTINCT cust_id)                    AS clientes,
# MAGIC        count(DISTINCT contract_id)                AS contratos,
# MAGIC        count(DISTINCT to_date(alta_ts))           AS dias_con_alta,
# MAGIC        min(alta_ts)                               AS primera_alta,
# MAGIC        max(alta_ts)                               AS ultima_alta,
# MAGIC        percentile_approx(timestampdiff(MINUTE, inicio_ts, baja_ts), 0.5) AS mediana_min_de_vida
# MAGIC FROM silver.periodos
# MAGIC WHERE isdn9 IN ('929340007', '901695738', '900583563', '916050441', '916900441')
# MAGIC   AND tipo = 'POS'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY isdn9, periodos DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- c) qué acciones tienen esas líneas
# MAGIC SELECT a.ACTION_CODE, c.nombre, count(*) AS eventos, count(DISTINCT a.PK_ID) AS lineas
# MAGIC FROM silver.periodos p
# MAGIC JOIN bronze.cm_pos__action_audit a ON a.PK_ID = p.sub_id AND a.PK_TYPE = '3'
# MAGIC LEFT JOIN silver.v_catalogo_acciones c ON c.origen = 'POS' AND c.action_code = a.ACTION_CODE
# MAGIC WHERE p.isdn9 = '929340007' AND p.tipo = 'POS'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY eventos DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cierre del capítulo
# MAGIC Actualiza `gold.diccionario_columnas` (cap. 3) con lo que descubriste:
# MAGIC qué significa de verdad cada fecha de SUB_MB, cuál es la fuente confiable de la fecha de migración y cuánto se hereda el STA.
# MAGIC
# MAGIC ## Preguntas tipo examen
# MAGIC 1. ¿Qué diferencia hay entre `ROW_NUMBER`, `RANK` y `DENSE_RANK` cuando hay empates? ¿Cuál usarías para quedarte con un solo "último cambio"?
# MAGIC 2. Tienes una columna arreglo de structs. ¿Qué función usas para quedarte con los elementos que cumplen una condición sin hacer `explode`?
# MAGIC 3. ¿Por qué una UDF de Python suele ser más lenta que una función nativa?
# MAGIC 4. `count(DISTINCT x)` vs `approx_count_distinct(x)`: ¿cuándo usarías la segunda?
# MAGIC 5. Un join entre una tabla de 60 M y otra de 300 filas: ¿qué optimización aplica Spark automáticamente?
# MAGIC
# MAGIC <details><summary>Respuestas</summary>
# MAGIC
# MAGIC 1. `ROW_NUMBER` numera sin empates (1, 2, 3); `RANK` repite y salta (1, 1, 3); `DENSE_RANK` repite sin saltar (1, 1, 2). Para uno solo: `ROW_NUMBER` con un desempate en el `ORDER BY`.
# MAGIC 2. `filter(arreglo, x -> condición)`.
# MAGIC 3. Los datos tienen que salir del motor (JVM/Photon) a un proceso de Python, fila por fila o por lotes, y el optimizador no puede ver dentro de la función.
# MAGIC 4. Cuando basta un estimado y la tabla es muy grande: es mucho más barata.
# MAGIC 5. Broadcast join (la tabla chica se copia a todos los nodos).
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ══ Cierre del cap. 4 ══════════════════════════════════════════════════════════
# MAGIC -- Pasa a bronze lo que el cap. 4 midió, y regenera el diccionario.
# MAGIC -- Va después de todo el 04. Si vuelves a correr E3.6, vuelve a correr esta celda después.
# MAGIC
# MAGIC -- 1) f_estado_linea con un 4.o parámetro opcional: si la línea tiene fecha de baja.
# MAGIC --    Con 3 argumentos sigue funcionando: para STATUS 0 da la etiqueta que junta los dos casos.
# MAGIC CREATE OR REPLACE FUNCTION silver.f_estado_linea(
# MAGIC     origen STRING, status STRING, act_status STRING,
# MAGIC     con_baja BOOLEAN DEFAULT NULL COMMENT 'END_DATETIME IS NOT NULL; separa cambio de titular de migracion en STATUS 0')
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Estado final de la linea con STATUS, ACT_STATUS y si tiene baja; el mismo STATUS significa cosas distintas en PRE y POS. Detalle en gold.catalogo_valores'
# MAGIC RETURN CASE
# MAGIC   WHEN origen = 'PRE' THEN CASE status
# MAGIC     WHEN '2' THEN CASE WHEN act_status LIKE '%3%' THEN 'PREACTIVADA (kit sin primer uso)'
# MAGIC                        WHEN act_status LIKE '_4'  THEN 'ACTIVA sin saldo (bloqueada)'
# MAGIC                        WHEN act_status LIKE '%2%' THEN 'ACTIVA con bloqueo 2 vias'
# MAGIC                        WHEN act_status LIKE '%1%' THEN 'ACTIVA con bloqueo 1 via'
# MAGIC                        ELSE 'ACTIVA' END
# MAGIC     WHEN '1' THEN 'REGISTRADA SIN CONECTAR'
# MAGIC     WHEN '3' THEN CASE WHEN act_status LIKE '%3%' THEN 'CANCELADA sin primer uso (kit)' ELSE 'CANCELADA' END
# MAGIC     WHEN '0' THEN CASE WHEN act_status LIKE '%3%' THEN 'ELIMINADA sin primer uso (kit)'
# MAGIC                        WHEN con_baja = false  THEN 'CERRADA POR CAMBIO DE TITULAR (linea cedida)'
# MAGIC                        WHEN con_baja = true   THEN 'ELIMINADA (probable paso a pospago)'
# MAGIC                        ELSE 'CERRADA (cambio de titular si no tiene baja; si la tiene, probable paso a pospago)' END
# MAGIC     WHEN '4' THEN 'CANCELADA EN CIERRE MASIVO DE KITS (13/06/2019)'
# MAGIC     WHEN '9' THEN 'SIN SIGNIFICADO DOCUMENTADO (PRE 9)'
# MAGIC     ELSE concat('STATUS no esperado: ', coalesce(status, 'NULL')) END
# MAGIC   WHEN origen = 'POS' THEN CASE status
# MAGIC     WHEN '2' THEN CASE WHEN act_status LIKE '%3%' THEN 'ACTIVA con digito 3 en ACT_STATUS (fuera del catalogo)'
# MAGIC                        WHEN act_status LIKE '%2%' THEN 'ACTIVA con bloqueo 2 vias'
# MAGIC                        WHEN act_status LIKE '%1%' THEN 'ACTIVA con bloqueo 1 via'
# MAGIC                        ELSE 'ACTIVA' END
# MAGIC     WHEN '9' THEN 'PREACTIVADA (registrada, aun no activa)'
# MAGIC     WHEN '1' THEN 'REGISTRADA SIN CONECTAR'
# MAGIC     WHEN '3' THEN 'CANCELADA'
# MAGIC     WHEN '0' THEN CASE WHEN con_baja = false THEN 'CERRADA POR CAMBIO DE TITULAR (linea cedida)'
# MAGIC                        WHEN con_baja = true  THEN 'INACTIVA (paso a prepago)'
# MAGIC                        ELSE 'CERRADA (cambio de titular si no tiene baja; si la tiene, paso a prepago)' END
# MAGIC     WHEN '4' THEN 'CERRADA POR CAMBIO DE TITULAR (mecanismo antiguo, 2021)'
# MAGIC     ELSE concat('STATUS no esperado: ', coalesce(status, 'NULL')) END
# MAGIC   ELSE 'origen no valido (usa PRE o POS)'
# MAGIC END;
# MAGIC
# MAGIC -- 2) los valores que cambiaron en gold.catalogo_valores
# MAGIC MERGE INTO gold.catalogo_valores c
# MAGIC USING (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('cm_pos__sub_mb', 'STATUS', '0',
# MAGIC      'Cerrada. Sin END_DATETIME: linea cedida en un cambio de titular. Con END_DATETIME: paso a prepago (accion 167)',
# MAGIC      'E4 cierre: 282.210 de 295.820 POS 0 sin baja (95,4 %) son FROM_SUB_ID de CHANGE_OWNER_LOG (DB_TYPE 0); con baja solo 14 de 1,6 M. BCCS 3 casos: Transfer to prepaid',
# MAGIC      'datos + BCCS'),
# MAGIC     ('cm_pre__sub_mb', 'STATUS', '0',
# MAGIC      'Eliminada (BCCS Delete). Con ACT_STATUS 03: kit borrado sin usarse. Sin END_DATETIME: linea cedida en un cambio de titular. Con END_DATETIME: probable paso a pospago',
# MAGIC      'E4 cierre: 180.360 de 181.378 PRE 0 sin baja y sin kit (99,4 %) son FROM_SUB_ID de CHANGE_OWNER_LOG (DB_TYPE 1); con baja solo 89 de 2,3 M',
# MAGIC      'datos; paso a pospago por confirmar'),
# MAGIC     ('cm_pre__sub_mb', 'STATUS', '4',
# MAGIC      'Kits preactivados (ACT_STATUS 03) cancelados en lote: baja 13/06/2019 00:30:53',
# MAGIC      'E4.9: 1.466 de 1.468 lineas con ACT_STATUS 03; BCCS 5 casos Cancelled con la misma baja',
# MAGIC      'datos + BCCS'),
# MAGIC     ('cm_pre__sub_mb', 'STATUS', '1',
# MAGIC      'Registrada sin conectar; casi siempre un kit (ACT_STATUS 03)',
# MAGIC      'BI_REPORT: NOT CONNECT en los 3 paquetes LSC; E4.9: 5.179 de 5.212 con ACT_STATUS 03',
# MAGIC      'codigo + datos'),
# MAGIC     ('cm_pos__sub_mb', 'STATUS', '4',
# MAGIC      'Cerrada por cambio de titular con el mecanismo antiguo (8 lineas, la mayoria con baja 12/06/2021). El actual deja la linea cedida en STATUS 0 sin baja',
# MAGIC      'BCCS 7 casos: Changed contract owner; CHANGE_OWNER_LOG empieza el 10/11/2021',
# MAGIC      'alta')
# MAGIC   AS v(tabla, columna, valor, significado, evidencia, confianza)) v
# MAGIC ON c.tabla = v.tabla AND c.columna = v.columna AND c.valor = v.valor
# MAGIC WHEN MATCHED THEN UPDATE SET c.significado = v.significado, c.evidencia = v.evidencia, c.confianza = v.confianza;
# MAGIC
# MAGIC -- 3) comentarios de columna con lo medido
# MAGIC ALTER TABLE bronze.cm_pos__sub_mb ALTER COLUMN STATUS COMMENT
# MAGIC   'Estado POS: 2 activa; 3 cancelada; 1 registrada sin conectar (00 sin 666); 9 preactivada (666 sin 00); 0 cerrada: sin END_DATETIME = cedida en cambio de titular (95,4 %), con END_DATETIME = paso a prepago (167); 4 cambio de titular antiguo (8 lineas, 2021). Sin catalogo en AP_DOMAIN; 0, 4 y 9 no significan lo mismo que en prepago. Valores: gold.catalogo_valores. Estado final: silver.f_estado_linea(origen, STATUS, ACT_STATUS, END_DATETIME IS NOT NULL)';
# MAGIC ALTER TABLE bronze.cm_pre__sub_mb ALTER COLUMN STATUS COMMENT
# MAGIC   'Estado PRE: 2 activa (con 3 en ACT_STATUS = kit preactivado); 3 cancelada (incluye 4,07 M kits nunca usados); 1 registrada sin conectar (99 % kits); 0 eliminada: con ACT_STATUS 03 = kit borrado, sin END_DATETIME = cedida en cambio de titular (99,4 %), con END_DATETIME = probable paso a pospago; 4 kits cancelados en lote el 13/06/2019; 9 sin significado (2 lineas). Sin catalogo en AP_DOMAIN. Valores: gold.catalogo_valores. Estado final: silver.f_estado_linea(origen, STATUS, ACT_STATUS, END_DATETIME IS NOT NULL)';
# MAGIC
# MAGIC ALTER TABLE bronze.cm_pos__sub_mb ALTER COLUMN STA_DATETIME COMMENT
# MAGIC   'Alta del periodo, pero se hereda: el 28,22 % de las POS tiene un gemelo PRE con la misma alta (E4.3), y se repite en las cadenas de cambio de titular (E4.8). Altas nuevas (NEW_POST): el 00 calza a 15 min en 100 %. Portabilidad (MNP_NEW): el 00 va antes del STA en 92,3 % y el STA coincide con una accion 04 Change information (probable ejecucion de la portabilidad, E4.6). Inicio real: silver.periodos.inicio_ts';
# MAGIC ALTER TABLE bronze.cm_pre__sub_mb ALTER COLUMN STA_DATETIME COMMENT
# MAGIC   'Alta del periodo, pero se hereda en la migracion POS a PRE (E4.3). Altas nuevas: el 00 calza a 15 min en 96,1 % (E4.6). Inicio real: silver.periodos.inicio_ts';
# MAGIC
# MAGIC ALTER TABLE bronze.cm_pos__sub_mb ALTER COLUMN MIG_DATETIME COMMENT
# MAGIC   'Inicio real del periodo migrado PRE a POS: a 1 dia o menos de CM_POS.TRANSFER_REQUEST.REQUEST_DATETIME en 100 % (4,6 M) y a 15 min del evento PRE_2_POST del CRM en 99,1-100 % (E4.5); con gemelo, el 00 calza con MIG en 98-99,5 % (E4.6). BI_REPORT usa NVL(MIG_DATETIME, STA_DATETIME)';
# MAGIC ALTER TABLE bronze.cm_pre__sub_mb ALTER COLUMN MIG_DATETIME COMMENT
# MAGIC   'Inicio real del periodo migrado POS a PRE: a 1 dia o menos de CM_PRE.TRANSFER_REQUEST.REQUEST_DATETIME en 100 % (2,0 M) y a 15 min del evento POST_2_PRE del CRM en 100 % (E4.5). BI_REPORT usa NVL(MIG_DATETIME, STA_DATETIME)';
# MAGIC
# MAGIC ALTER TABLE bronze.cm_pos__sub_mb ALTER COLUMN END_DATETIME COMMENT
# MAGIC   'Baja del periodo. NULL = abierta, salvo con STATUS 0: ahi es una linea cedida en cambio de titular (cerrada, sin fecha)';
# MAGIC ALTER TABLE bronze.cm_pre__sub_mb ALTER COLUMN END_DATETIME COMMENT
# MAGIC   'Baja del periodo. NULL = abierta, salvo con STATUS 0: linea cedida en cambio de titular, o kit borrado si ACT_STATUS es 03';
# MAGIC
# MAGIC ALTER TABLE bronze.cm_pos__transfer_request ALTER COLUMN NEW_SUB_ID COMMENT
# MAGIC   'NO sirve como llave del pospago nuevo: es el SUB_ID POS del mismo numero en 18,4 % (0,7 % en 2026); 50,5 % viene vacio y 31,0 % copia OLD_SUB_ID. Cruza por ISDN + fecha (E4.5)';
# MAGIC ALTER TABLE bronze.cm_pos__transfer_request ALTER COLUMN STATUS COMMENT
# MAGIC   '0 y 1 terminan en migracion en 98,1 % y 99,8 %: no indica exito. 1 sin uso desde el 16/10/2026. Sin catalogo (E4.5)';
# MAGIC ALTER TABLE bronze.cm_pre__transfer_request ALTER COLUMN NEW_SUB_ID COMMENT
# MAGIC   'SUB_ID del prepago nuevo: existe en PRE en 98,9 % y calza con el evento POST_2_PRE del CRM (E4.5)';
# MAGIC ALTER TABLE bronze.cm_pre__transfer_request ALTER COLUMN STATUS COMMENT
# MAGIC   '0 y 1 terminan en migracion en 98,5 % y 94,7 %. 1 sin uso desde el 17/06/2025. Hay REQUEST_DATETIME futuras (hasta 28/11/2026). Sin catalogo (E4.5)';
# MAGIC ALTER TABLE bronze.cm_pre__request_transfer_postpaid ALTER COLUMN STATUS COMMENT
# MAGIC   'Cola de solicitudes PRE a POS, sin catalogo (E4.5). 0 en cola (las 17 son de minutos antes del corte); 1 (desde 2020) y 3 (desde 05/2025) procesadas: 99,9 % y 92,1 % migran; -1 y 2: 28-33 % no migran (rechazo o reintento probable); 9 lote del 26-29/04/2025';
# MAGIC
# MAGIC ALTER TABLE bronze.cm_pos__change_owner_log ALTER COLUMN DB_TYPE COMMENT '0 pospago, 1 prepago (E4.7)';
# MAGIC ALTER TABLE bronze.cm_pos__change_owner_log ALTER COLUMN FROM_SUB_ID COMMENT
# MAGIC   'Linea cedida: queda en SUB_MB con STATUS 0 y sin END_DATETIME (95,4 % POS, 99,4 % PRE). Siempre distinto de TO_SUB_ID';
# MAGIC ALTER TABLE bronze.cm_pos__change_owner_log ALTER COLUMN TO_SUB_ID COMMENT
# MAGIC   'Linea nueva del nuevo titular (SUB_ID nuevo). El ultimo cambio de cada numero coincide con el titular actual en 99,7 % (E4.7)';
# MAGIC
# MAGIC ALTER TABLE bronze.cm_pos__action_audit ALTER COLUMN PK_TYPE COMMENT '3 = linea: PK_ID es SUB_ID (90,7 % existe en SUB_MB; E4.6)';
# MAGIC ALTER TABLE bronze.cm_pre__action_audit ALTER COLUMN PK_TYPE COMMENT '3 = linea: PK_ID es SUB_ID (100 % existe en SUB_MB; E4.6)';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4) el diccionario recoge los comentarios nuevos (mismo CTAS de E3.6, celda 6)
# MAGIC CREATE OR REPLACE TABLE gold.diccionario_columnas
# MAGIC COMMENT 'Diccionario de columnas de bronze (information_schema + comentarios). Se regenera con este CTAS'
# MAGIC AS
# MAGIC SELECT table_catalog, table_schema, table_name, column_name, ordinal_position,
# MAGIC        data_type, is_nullable, comment
# MAGIC FROM workspace.information_schema.columns
# MAGIC WHERE table_schema = 'bronze';
# MAGIC
# MAGIC -- 5) terminado cuando: las columnas de arriba tienen su comentario nuevo y STATUS 0 se separa en dos
# MAGIC SELECT table_name, column_name, left(comment, 90) AS comentario
# MAGIC FROM gold.diccionario_columnas
# MAGIC WHERE (table_name LIKE '%sub_mb'             AND column_name IN ('STATUS', 'STA_DATETIME', 'MIG_DATETIME', 'END_DATETIME'))
# MAGIC    OR (table_name LIKE '%transfer%'           AND column_name IN ('NEW_SUB_ID', 'STATUS'))
# MAGIC    OR (table_name = 'cm_pos__change_owner_log' AND column_name IN ('DB_TYPE', 'FROM_SUB_ID', 'TO_SUB_ID'))
# MAGIC ORDER BY 1, 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tipo, status, silver.f_estado_linea(tipo, status, act_status, baja_ts IS NOT NULL) AS estado_final, count(*) AS lineas
# MAGIC FROM silver.lineas
# MAGIC WHERE status IN ('0', '4')
# MAGIC GROUP BY ALL
# MAGIC ORDER BY tipo, lineas DESC;