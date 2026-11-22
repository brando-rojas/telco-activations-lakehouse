# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
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

# Antes de E8.1 · ¿existen las tablas y columnas que usa la línea de tiempo? (corregido con tu salida real)
# PORTING_REQUEST, TERMINATION_SUB y SUB_SIM_TRANSFER no traen SUB_ID: se cruzan por número + fecha (silver.tenencia)
necesarias = {
    "bronze.mnp__porting_request":     ["PORT_NUMBER", "TYPE", "STATE_CODE", "EXECUTION_DATE", "CREATE_DATE",
                                        "TRANSFEROR_CODE", "RECEIVER_CODE"],
    "bronze.cm_pos__change_owner_log": ["ISDN", "FROM_SUB_ID", "TO_SUB_ID", "FROM_CUST_ID", "TO_CUST_ID",
                                        "DB_TYPE", "CREATE_TIME"],
    "bronze.cm_pos__terminate_req":    ["ISDN", "SUB_ID", "DB_TYPE", "CREATE_TIME", "STATUS", "EXECUTE_DATE", "REASON_ID"],
    "bronze.cm_pre__termination_sub":  ["ISDN", "IMPORT_DATETIME", "TERMINATE_TIME", "STATE", "LAST_STATE",
                                        "TERMINATE_REASON", "REASON"],
    "bronze.cm_pos__req_change_sim":   ["ISDN", "SUB_ID", "PROCESS_DATE", "CREATE_TIME", "STATUS", "DB_TYPE"],
    "bronze.cm_pre__sub_sim_transfer": ["ISDN", "CHANGE_DATETIME", "END_DATETIME", "STATUS"],
    "bronze.cm_pos__sub_change_isdn":  ["SUB_ID", "OLD_ISDN", "NEW_ISDN", "DB_TYPE", "PROCESS_DATE", "CREATE_TIME", "STATUS"],
    "bronze.cm_pos__auto_terminate_sub": ["SUB_ID"],
    "bronze.cm_pos__auto_cancel_sub":    ["SUB_ID"],
    # lo que se arma en el cap. 4 y se reutiliza aquí
    "silver.lineas":   ["TIPO", "SUB_ID", "ISDN9", "CUST_ID", "STATUS", "ACT_STATUS", "BAJA_TS", "MIG_TS", "ALTA_TS"],
    "silver.periodos": ["TIPO", "SUB_ID", "ISDN9", "CUST_ID", "STATUS", "BAJA_TS", "ALTA_TXT", "INICIO_TS", "FUENTE_INICIO"],
    "silver.transiciones": ["ISDN9", "SUB_ANT", "SUB_ID", "TIPO_ANT", "TIPO", "TRANSICION", "BAJA_ANT", "INICIO_TS",
                            "DIAS", "MISMO_CLIENTE"],
}
for tabla, cols in necesarias.items():
    try:
        hay = {c.upper() for c in spark.table(tabla).columns}
    except Exception:
        print(f"✗ {tabla}: NO EXISTE")
        continue
    faltan = [c for c in cols if c not in hay]
    print(f"✓ {tabla}" if not faltan else f"✗ {tabla}  faltan {faltan}\n    tiene: {sorted(hay)}")
# Todo tiene que salir ✓. Si alguna sale ✗, mándame la salida antes de E8.1.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E8.0 a) Catálogo de estados (MNP.STATE)
# MAGIC - Los códigos tienen nombre y descripción: el catálogo sí sirve.
# MAGIC - Estados de cierre exitoso: **00A02** "Process finished successfully" y **01A06** "Porting successfully completed".
# MAGIC - Prefijo 05 = consulta previa (por ejemplo, 05A03 "Consulta previa correcta"). Prefijo 02 = retorno de número (02R02 "refuse return", 02D04 "Sent Number Return to Original Assignee"). -1 = cancelada.
# MAGIC - **BYPASS** y **M_BYPASS** no están en el catálogo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- E8.0 · a) el catálogo de estados de portabilidad (es chico: míralo entero)
# MAGIC SELECT * FROM bronze.mnp__state ORDER BY 1;

# COMMAND ----------

# MAGIC  %md
# MAGIC ### E8.0 b) Qué es "ejecutada" y quién es Bitel
# MAGIC - Bitel es el operador **24**, y el TYPE ya dice el sentido: en TYPE 01 y 02 Bitel recibe el 100%; en TYPE 05 y 06 Bitel cede (salvo 419 consultas previas).
# MAGIC - Solo **00A02 y 01A06** tienen fecha de ejecución de forma sistemática. Esos son los "ejecutados" que usa E8.1.
# MAGIC
# MAGIC | TYPE | Significado | Ejecutadas | Lo demás |
# MAGIC |---|---|---|---|
# MAGIC | 01 | Portabilidad de entrada | 883.364 | 617 mil consultas previas, 262 mil canceladas, **1,03 M en BYPASS sin fecha** |
# MAGIC | 05 | Portabilidad de salida | 996.675 | 253 mil consultas previas de otros operadores |
# MAGIC | 02 | El número vuelve a Bitel (retorno) | 393.212 | 253 mil retornos rechazados |
# MAGIC | 06 | Bitel devuelve el número a su operador original | 382.914 | 8.343 en 02D04 (tienen fecha, pero el estado es de rechazo: quedan fuera) |
# MAGIC
# MAGIC - En todo el histórico del export, Bitel cedió unas 113 mil líneas más de las que recibió por portabilidad.
# MAGIC - TYPE 02 y 06 se interpretan como retorno por los nombres del catálogo y por el sentido 24: quien recibe o cede.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- E8.0 · b) qué estado significa "ejecutada". TYPE 01 = entrada, 05 = salida. Bitel es el operador 24:
# MAGIC --           en una entrada RECEIVER_CODE = 24 y en una salida TRANSFEROR_CODE = 24 (control cruzado de TYPE)
# MAGIC SELECT TYPE, STATE_CODE, count(*) AS solicitudes, count(EXECUTION_DATE) AS con_fecha_ejecucion,
# MAGIC        count_if(RECEIVER_CODE = '24') AS bitel_recibe, count_if(TRANSFEROR_CODE = '24') AS bitel_cede
# MAGIC FROM bronze.mnp__porting_request
# MAGIC GROUP BY TYPE, STATE_CODE
# MAGIC ORDER BY TYPE, solicitudes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### E8.0 c) PORT_NUMBER contra SUB_MB
# MAGIC - PORT_NUMBER calza con un número de SUB_MB en el 93,2% de TYPE 01 y en 99,9–100% de los demás: `f_isdn9` lo limpia bien.
# MAGIC - El 6,8% que no calza en TYPE 01 son sobre todo consultas y cancelaciones: el número nunca llegó a Bitel.
# MAGIC - Como PORTING_REQUEST no tiene SUB_ID, la línea se encuentra por número + fecha de ejecución (`silver.tenencia`, E8.1-a).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- E8.0 · c) PORTING_REQUEST no trae SUB_ID ni ISDN: el número es PORT_NUMBER. ¿Cuántas solicitudes calzan
# MAGIC --           con un número que existe en SUB_MB? (las rechazadas pueden no calzar: la línea nunca llegó a Bitel)
# MAGIC WITH numeros AS (SELECT DISTINCT isdn9 FROM silver.lineas WHERE isdn9 IS NOT NULL AND isdn9 <> '')
# MAGIC SELECT p.TYPE, count(*) AS solicitudes, count(n.isdn9) AS numero_en_lineas,
# MAGIC        round(100 * count(n.isdn9) / count(*), 1) AS pct
# MAGIC FROM bronze.mnp__porting_request p
# MAGIC LEFT JOIN numeros n ON n.isdn9 = nullif(silver.f_isdn9(p.PORT_NUMBER), '')
# MAGIC GROUP BY p.TYPE
# MAGIC ORDER BY p.TYPE;

# COMMAND ----------

# MAGIC %md
# MAGIC ### E8.0 d) ¿Los BYPASS son portabilidades reales?
# MAGIC - Con 00A02, en el **99,8%** de los casos nace una línea Bitel en los 10 días siguientes a la solicitud; con BYPASS, en el **87,7%**; con M_BYPASS, en el **81,8%**.
# MAGIC - Conclusión: **sí son entradas reales** (~866 mil), solo que no tienen fecha de ejecución ni estado en el catálogo. E8.1 las incluye con la fecha de la solicitud y lo marca en el detalle (`fecha=solicitud (BYPASS sin ejecucion)`).
# MAGIC - Con ellas, las entradas por portabilidad pasan de ~883 mil a ~1,9 M.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TYPE 01 tiene ~1 M de solicitudes en BYPASS / M_BYPASS: no están en MNP.STATE y no tienen fecha de ejecución.
# MAGIC -- Si una línea Bitel nace con ese número poco después de la solicitud, igual que con 00A02, son entradas reales.
# MAGIC WITH b AS (
# MAGIC   SELECT monotonically_increasing_id() AS id, STATE_CODE,
# MAGIC          nullif(silver.f_isdn9(PORT_NUMBER), '') AS isdn9,
# MAGIC          silver.f_ts(CREATE_DATE) AS creada_ts, PORT_IN_ACTIVE_DATE
# MAGIC   FROM bronze.mnp__porting_request
# MAGIC   WHERE TYPE = '01' AND STATE_CODE IN ('00A02', 'BYPASS', 'M_BYPASS'))
# MAGIC SELECT b.STATE_CODE, count(DISTINCT b.id) AS solicitudes,
# MAGIC        count(DISTINCT CASE WHEN b.PORT_IN_ACTIVE_DATE IS NOT NULL THEN b.id END) AS con_port_in_active_date,
# MAGIC        count(DISTINCT CASE WHEN p.sub_id IS NOT NULL THEN b.id END) AS linea_nace_en_10_dias,
# MAGIC        round(100 * count(DISTINCT CASE WHEN p.sub_id IS NOT NULL THEN b.id END) / count(DISTINCT b.id), 1) AS pct
# MAGIC FROM b
# MAGIC LEFT JOIN silver.periodos p
# MAGIC   ON p.isdn9 = b.isdn9
# MAGIC  AND p.inicio_ts BETWEEN b.creada_ts - INTERVAL 1 DAY AND b.creada_ts + INTERVAL 10 DAYS
# MAGIC GROUP BY b.STATE_CODE
# MAGIC ORDER BY b.STATE_CODE;

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
# MAGIC -- Por qué existe: PORTING_REQUEST, CM_PRE.TERMINATION_SUB y CM_PRE.SUB_SIM_TRANSFER solo traen número + fecha.
# MAGIC -- Para saber de qué línea es cada evento hay que preguntar "¿qué SUB_ID tenía este número en esta fecha?".
# MAGIC -- Esa respuesta se arma UNA vez aquí, con las reglas que ya validaste, y todo lo demás la reutiliza:
# MAGIC --   desde = inicio real del cap. 4 (silver.periodos: MIG_DATETIME / baja del gemelo / STA);
# MAGIC --           si la línea nació de un cambio de titular, la fecha del cambio (su STA es heredado)
# MAGIC --   hasta = la baja; si no tiene: la fecha en que se cedió (CHANGE_OWNER_LOG); si no: el inicio de la siguiente
# MAGIC --           línea del número; si no hay nada: NULL (vigente, o cerrada sin fecha: fuente_hasta lo dice)
# MAGIC
# MAGIC -- ── CELDA E8.1-a (%sql) ──
# MAGIC CREATE OR REPLACE TABLE silver.tenencia
# MAGIC CLUSTER BY (isdn9)
# MAGIC COMMENT 'Quien tuvo cada numero y desde/hasta cuando (un periodo por linea). Para cruzar tablas que solo traen numero + fecha'
# MAGIC AS
# MAGIC WITH cambios AS (
# MAGIC   SELECT CASE DB_TYPE WHEN '0' THEN 'POS' WHEN '1' THEN 'PRE' END AS tipo, FROM_SUB_ID, TO_SUB_ID,
# MAGIC          silver.f_ts(CREATE_TIME) AS cambio_ts
# MAGIC   FROM bronze.cm_pos__change_owner_log
# MAGIC   WHERE TO_SUB_ID <> FROM_SUB_ID),
# MAGIC recibidas AS (SELECT tipo, TO_SUB_ID AS sub_id, min(cambio_ts) AS recibida_ts FROM cambios GROUP BY tipo, TO_SUB_ID),
# MAGIC cedidas   AS (SELECT tipo, FROM_SUB_ID AS sub_id, min(cambio_ts) AS cedida_ts FROM cambios GROUP BY tipo, FROM_SUB_ID),
# MAGIC base AS (
# MAGIC   SELECT p.isdn9, p.sub_id, p.tipo, p.cust_id, p.status, p.alta_txt, p.baja_ts, c.cedida_ts,
# MAGIC          CASE WHEN r.recibida_ts > p.inicio_ts THEN r.recibida_ts ELSE p.inicio_ts END AS desde,
# MAGIC          CASE WHEN r.recibida_ts > p.inicio_ts THEN 'CAMBIO_TITULAR' ELSE p.fuente_inicio END AS fuente_desde
# MAGIC   FROM silver.periodos p
# MAGIC   LEFT JOIN recibidas r ON r.tipo = p.tipo AND r.sub_id = p.sub_id
# MAGIC   LEFT JOIN cedidas   c ON c.tipo = p.tipo AND c.sub_id = p.sub_id
# MAGIC   WHERE p.isdn9 IS NOT NULL AND p.isdn9 <> '' AND p.inicio_ts IS NOT NULL),
# MAGIC con_siguiente AS (
# MAGIC   SELECT *, LEAD(desde) OVER (PARTITION BY isdn9 ORDER BY desde, tipo, sub_id) AS desde_siguiente FROM base)
# MAGIC SELECT isdn9, sub_id, tipo, cust_id, status, alta_txt, desde, fuente_desde,
# MAGIC        coalesce(baja_ts, cedida_ts, desde_siguiente) AS hasta,
# MAGIC        CASE WHEN baja_ts IS NOT NULL         THEN 'BAJA'
# MAGIC             WHEN cedida_ts IS NOT NULL       THEN 'CAMBIO_TITULAR'
# MAGIC             WHEN desde_siguiente IS NOT NULL THEN 'SIGUIENTE_LINEA'
# MAGIC             WHEN status IN ('1', '2', '9')   THEN 'VIGENTE'
# MAGIC             ELSE 'CERRADA_SIN_FECHA' END AS fuente_hasta
# MAGIC FROM con_siguiente;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.1-a2 (%sql): ¿qué tan sólido es el historial? ──
# MAGIC -- hasta_antes_de_desde y se_solapa_con_el_anterior deberían ser pocos.
# MAGIC -- SIGUIENTE_LINEA con la línea aún activa (status 2) = el número quedó en dos líneas a la vez: son los casos de R5 (E8.2)
# MAGIC WITH s AS (
# MAGIC   SELECT *, LAG(hasta) OVER (PARTITION BY isdn9 ORDER BY desde, tipo, sub_id) AS hasta_anterior
# MAGIC   FROM silver.tenencia)
# MAGIC SELECT tipo, fuente_desde, fuente_hasta, count(*) AS periodos,
# MAGIC        count_if(hasta < desde) AS hasta_antes_de_desde,
# MAGIC        count_if(desde < hasta_anterior) AS se_solapa_con_el_anterior
# MAGIC FROM s
# MAGIC GROUP BY tipo, fuente_desde, fuente_hasta
# MAGIC ORDER BY tipo, periodos DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE TABLE gold.linea_tiempo CLUSTER BY (isdn9) AS
# MAGIC -- WITH altas AS (...), bajas AS (...), migraciones AS (...), ...
# MAGIC -- SELECT * FROM altas UNION ALL SELECT * FROM bajas UNION ALL ...
# MAGIC -- Las tablas con SUB_ID entran directo. Las que solo traen número (portabilidad, TERMINATION_SUB, SUB_SIM_TRANSFER)
# MAGIC -- pasan por una sola regla de cruce con silver.tenencia:
# MAGIC --   1) mismo número (y tipo PRE para las tablas de CM_PRE)
# MAGIC --   2) gana el periodo que CONTIENE la fecha; si ninguno, el más cercano a 3 días o menos; si empatan, el más reciente
# MAGIC --   3) el detalle dice cómo se encontró: linea=dentro del periodo · linea=a N min / N h del periodo · linea=ninguna a 3 dias
# MAGIC --      (no se pierde ningún evento: si no hay línea, queda con sub_id NULL)
# MAGIC
# MAGIC -- ── CELDA E8.1 (%sql) ──
# MAGIC CREATE OR REPLACE TABLE gold.linea_tiempo
# MAGIC CLUSTER BY (isdn9)
# MAGIC COMMENT 'Un evento por fila de cada numero, con su fecha real, la linea (sub_id) que lo tenia y la tabla de donde sale'
# MAGIC AS
# MAGIC WITH
# MAGIC ejecutada AS (SELECT * FROM VALUES ('00A02'), ('01A06'), ('BYPASS'), ('M_BYPASS') AS v(state_code)),  -- E8.0 b) y d)
# MAGIC cambios AS (
# MAGIC   -- CHANGE_OWNER_LOG: FROM_SUB_ID = línea cedida (queda STATUS 0 sin baja); TO_SUB_ID = línea nueva (hereda el STA)
# MAGIC   SELECT nullif(silver.f_isdn9(ISDN), '') AS isdn9, FROM_SUB_ID, TO_SUB_ID, FROM_CUST_ID, TO_CUST_ID,
# MAGIC          CASE DB_TYPE WHEN '0' THEN 'POS' WHEN '1' THEN 'PRE' END AS tipo,
# MAGIC          silver.f_ts(CREATE_TIME) AS cambio_ts
# MAGIC   FROM bronze.cm_pos__change_owner_log),
# MAGIC recibidas AS (SELECT DISTINCT tipo, TO_SUB_ID AS sub_id FROM cambios WHERE TO_SUB_ID <> FROM_SUB_ID),
# MAGIC cedidas   AS (SELECT DISTINCT tipo, FROM_SUB_ID AS sub_id FROM cambios WHERE TO_SUB_ID <> FROM_SUB_ID),
# MAGIC -- · tablas con SUB_ID propio ·
# MAGIC altas AS (
# MAGIC   -- el inicio real ya viene resuelto en silver.tenencia (regla del cap. 4 + cambio de titular)
# MAGIC   SELECT isdn9, sub_id, tipo AS tipo_linea, 'ALTA' AS tipo_evento, desde AS fecha_ts, 'silver.tenencia' AS fuente,
# MAGIC          concat_ws('; ', 'inicio segun ' || fuente_desde, 'STA=' || alta_txt) AS detalle
# MAGIC   FROM silver.tenencia),
# MAGIC bajas AS (
# MAGIC   -- solo las que tienen fecha: las cedidas (STATUS 0 sin baja) salen como CAMBIO_TITULAR
# MAGIC   SELECT isdn9, sub_id, tipo, 'BAJA', baja_ts, 'silver.lineas',
# MAGIC          concat_ws('; ', 'STATUS=' || status, 'ACT_STATUS=' || act_status,
# MAGIC                    silver.f_estado_linea(tipo, status, act_status, baja_ts IS NOT NULL))   -- el estado en palabras
# MAGIC   FROM silver.lineas
# MAGIC   WHERE baja_ts IS NOT NULL),
# MAGIC migraciones AS (
# MAGIC   SELECT isdn9, sub_id, tipo, 'MIGRACION', inicio_ts, 'silver.transiciones',
# MAGIC          concat_ws('; ', transicion, 'desde sub ' || sub_ant, 'dias=' || dias)
# MAGIC   FROM silver.transiciones
# MAGIC   WHERE tipo_ant <> tipo AND mismo_cliente),
# MAGIC reciclajes AS (
# MAGIC   -- otro cliente con la línea anterior YA dada de baja; sin los cambios de titular (tienen su propio evento)
# MAGIC   SELECT t.isdn9, t.sub_id, t.tipo, 'RECICLAJE', t.inicio_ts, 'silver.transiciones',
# MAGIC          concat_ws('; ', t.transicion, 'libre ' || t.dias || ' dias')
# MAGIC   FROM silver.transiciones t
# MAGIC   LEFT ANTI JOIN cedidas   c ON c.tipo = t.tipo_ant AND c.sub_id = t.sub_ant
# MAGIC   LEFT ANTI JOIN recibidas r ON r.tipo = t.tipo     AND r.sub_id = t.sub_id
# MAGIC   WHERE NOT t.mismo_cliente AND t.baja_ant IS NOT NULL),
# MAGIC cambio_titular AS (
# MAGIC   SELECT isdn9, TO_SUB_ID, tipo, 'CAMBIO_TITULAR', cambio_ts, 'cm_pos.change_owner_log',
# MAGIC          concat_ws('; ', 'de sub ' || FROM_SUB_ID, 'a sub ' || TO_SUB_ID,
# MAGIC                    'de cliente ' || FROM_CUST_ID, 'a cliente ' || TO_CUST_ID)
# MAGIC   FROM cambios),
# MAGIC solicitud_baja_pos AS (
# MAGIC   -- TERMINATE_REQ no tiene REQ_DATE: la solicitud es CREATE_TIME y EXECUTE_DATE va al detalle.
# MAGIC   -- Trae DB_TYPE: también puede tener prepago (0 = pospago, 1 = prepago, como en CHANGE_OWNER_LOG)
# MAGIC   SELECT silver.f_isdn9(ISDN), SUB_ID, CASE DB_TYPE WHEN '0' THEN 'POS' WHEN '1' THEN 'PRE' END, 'SOLICITUD_BAJA',
# MAGIC          silver.f_ts(CREATE_TIME), 'cm_pos.terminate_req',
# MAGIC          concat_ws('; ', 'STATUS=' || STATUS, 'ejecutar=' || EXECUTE_DATE, 'motivo=' || REASON_ID)
# MAGIC   FROM bronze.cm_pos__terminate_req),
# MAGIC cambio_sim_pos AS (
# MAGIC   SELECT silver.f_isdn9(ISDN), SUB_ID, 'POS', 'CAMBIO_SIM',
# MAGIC          coalesce(silver.f_ts(PROCESS_DATE), silver.f_ts(CREATE_TIME)), 'cm_pos.req_change_sim',
# MAGIC          concat_ws('; ', 'STATUS=' || STATUS, 'DB_TYPE=' || DB_TYPE)
# MAGIC   FROM bronze.cm_pos__req_change_sim),
# MAGIC cambio_numero AS (
# MAGIC   -- un evento por cada número involucrado (el viejo y el nuevo). DB_TYPE 0 = pospago, 1 = prepago; STATUS 1 = éxito
# MAGIC   SELECT silver.f_isdn9(n.isdn), SUB_ID, CASE DB_TYPE WHEN '0' THEN 'POS' WHEN '1' THEN 'PRE' END,
# MAGIC          'CAMBIO_NUMERO', coalesce(silver.f_ts(PROCESS_DATE), silver.f_ts(CREATE_TIME)),
# MAGIC          'cm_pos.sub_change_isdn',
# MAGIC          concat_ws('; ', n.rol, 'de ' || OLD_ISDN, 'a ' || NEW_ISDN, 'STATUS=' || STATUS)
# MAGIC   FROM bronze.cm_pos__sub_change_isdn
# MAGIC   LATERAL VIEW explode(array(named_struct('rol', 'numero anterior', 'isdn', OLD_ISDN),
# MAGIC                              named_struct('rol', 'numero nuevo',    'isdn', NEW_ISDN))) x AS n
# MAGIC   WHERE STATUS = '1'),
# MAGIC -- · tablas con solo número + fecha: se busca la línea en silver.tenencia ·
# MAGIC por_numero AS (
# MAGIC   SELECT monotonically_increasing_id() AS id, * FROM (
# MAGIC     -- portabilidad y retorno de número: la fecha es la ejecución (en portabilidad el STA de la línea es esa fecha, cap. 4)
# MAGIC     SELECT nullif(silver.f_isdn9(p.PORT_NUMBER), '') AS isdn9, CAST(NULL AS STRING) AS tipo_req,
# MAGIC            CASE p.TYPE WHEN '01' THEN 'PORTABILIDAD_IN'  WHEN '05' THEN 'PORTABILIDAD_OUT'
# MAGIC                        WHEN '02' THEN 'RETORNO_IN'       WHEN '06' THEN 'RETORNO_OUT' END AS tipo_evento,
# MAGIC            coalesce(silver.f_ts(p.EXECUTION_DATE), silver.f_ts(p.CREATE_DATE)) AS fecha_ts, 'mnp.porting_request' AS fuente,
# MAGIC            concat_ws('; ', 'STATE_CODE=' || p.STATE_CODE, 'cedente=' || p.TRANSFEROR_CODE,
# MAGIC                      'receptor=' || p.RECEIVER_CODE, 'solicitud=' || p.CREATE_DATE,
# MAGIC                      CASE WHEN p.EXECUTION_DATE IS NULL THEN 'fecha=solicitud (BYPASS sin ejecucion)' END) AS detalle
# MAGIC     FROM bronze.mnp__porting_request p
# MAGIC     JOIN ejecutada e ON e.state_code = p.STATE_CODE
# MAGIC     WHERE p.TYPE IN ('01', '05', '02', '06')     -- E8.0: 02 = el número vuelve a Bitel, 06 = Bitel lo devuelve
# MAGIC     UNION ALL
# MAGIC     -- baja prepago: IMPORT_DATETIME = cuándo se pidió (la línea aún era del titular); TERMINATE_TIME = cuándo se ejecutó
# MAGIC     SELECT nullif(silver.f_isdn9(ISDN), ''), 'PRE', 'SOLICITUD_BAJA',
# MAGIC            coalesce(silver.f_ts(IMPORT_DATETIME), silver.f_ts(TERMINATE_TIME)), 'cm_pre.termination_sub',
# MAGIC            concat_ws('; ', 'ejecutada=' || TERMINATE_TIME, 'STATE=' || STATE, 'LAST_STATE=' || LAST_STATE,
# MAGIC                      'motivo=' || coalesce(TERMINATE_REASON, REASON))
# MAGIC     FROM bronze.cm_pre__termination_sub
# MAGIC     UNION ALL
# MAGIC     -- SIM prepago: una fila por SIM que tuvo el número, desde CHANGE_DATETIME hasta END_DATETIME
# MAGIC     SELECT nullif(silver.f_isdn9(ISDN), ''), 'PRE', 'CAMBIO_SIM',
# MAGIC            silver.f_ts(CHANGE_DATETIME), 'cm_pre.sub_sim_transfer',
# MAGIC            concat_ws('; ', 'STATUS=' || STATUS, 'SIM hasta=' || END_DATETIME)
# MAGIC     FROM bronze.cm_pre__sub_sim_transfer)
# MAGIC   WHERE isdn9 IS NOT NULL AND fecha_ts IS NOT NULL),
# MAGIC candidatos AS (
# MAGIC   SELECT e.*, t.sub_id, t.tipo, t.desde,
# MAGIC          e.fecha_ts >= t.desde AND (t.hasta IS NULL OR e.fecha_ts < t.hasta) AS dentro,
# MAGIC          least(abs(unix_timestamp(e.fecha_ts) - unix_timestamp(t.desde)),
# MAGIC                abs(unix_timestamp(e.fecha_ts) - unix_timestamp(coalesce(t.hasta, t.desde)))) AS distancia_s
# MAGIC   FROM por_numero e
# MAGIC   LEFT JOIN silver.tenencia t
# MAGIC     ON t.isdn9 = e.isdn9
# MAGIC    AND (e.tipo_req IS NULL OR t.tipo = e.tipo_req)
# MAGIC    AND e.fecha_ts >= t.desde - INTERVAL 3 DAYS
# MAGIC    AND (t.hasta IS NULL OR e.fecha_ts < t.hasta + INTERVAL 3 DAYS)),
# MAGIC asignados AS (
# MAGIC   SELECT isdn9, sub_id, coalesce(tipo, tipo_req), tipo_evento, fecha_ts, fuente,
# MAGIC          concat_ws('; ', detalle,
# MAGIC                    CASE WHEN sub_id IS NULL THEN 'linea=ninguna a 3 dias'
# MAGIC                         WHEN dentro         THEN 'linea=dentro del periodo'
# MAGIC                         WHEN distancia_s < 3600 THEN 'linea=a ' || CAST(round(distancia_s / 60) AS INT) || ' min del periodo'
# MAGIC                         ELSE 'linea=a ' || round(distancia_s / 3600, 1) || ' h del periodo' END,
# MAGIC                    CASE WHEN tipo_evento = 'CAMBIO_SIM'
# MAGIC                          AND abs(unix_timestamp(fecha_ts) - unix_timestamp(desde)) < 86400 THEN 'SIM del alta' END)
# MAGIC   FROM (SELECT c.*, ROW_NUMBER() OVER (PARTITION BY id
# MAGIC                                        ORDER BY dentro DESC NULLS LAST, distancia_s NULLS LAST, desde DESC) AS rn
# MAGIC         FROM candidatos c)
# MAGIC   WHERE rn = 1),
# MAGIC todos AS (
# MAGIC   SELECT * FROM altas
# MAGIC   UNION ALL SELECT * FROM bajas
# MAGIC   UNION ALL SELECT * FROM migraciones
# MAGIC   UNION ALL SELECT * FROM reciclajes
# MAGIC   UNION ALL SELECT * FROM cambio_titular
# MAGIC   UNION ALL SELECT * FROM solicitud_baja_pos
# MAGIC   UNION ALL SELECT * FROM cambio_sim_pos
# MAGIC   UNION ALL SELECT * FROM cambio_numero
# MAGIC   UNION ALL SELECT * FROM asignados)
# MAGIC SELECT * FROM todos
# MAGIC WHERE isdn9 IS NOT NULL AND isdn9 <> '' AND fecha_ts IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- terminado cuando: cada fuente aporta filas
# MAGIC SELECT fuente, tipo_evento, count(*) AS eventos, count(DISTINCT isdn9) AS numeros,
# MAGIC        min(fecha_ts) AS desde, max(fecha_ts) AS hasta
# MAGIC FROM gold.linea_tiempo
# MAGIC GROUP BY fuente, tipo_evento
# MAGIC ORDER BY fuente, tipo_evento;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.1-b (%sql): ¿qué tan bien se encontró la línea de los eventos que solo traen número? ──
# MAGIC SELECT fuente, tipo_evento, count(*) AS eventos,
# MAGIC        count_if(detalle LIKE '%linea=dentro del periodo%') AS dentro_del_periodo,
# MAGIC        count_if(detalle LIKE '%linea=a %')                 AS cerca_a_3_dias,
# MAGIC        count_if(sub_id IS NULL)                            AS sin_linea,
# MAGIC        count_if(detalle LIKE '%SIM del alta%')             AS sim_del_alta
# MAGIC FROM gold.linea_tiempo
# MAGIC WHERE fuente IN ('mnp.porting_request', 'cm_pre.termination_sub', 'cm_pre.sub_sim_transfer')
# MAGIC GROUP BY fuente, tipo_evento
# MAGIC ORDER BY fuente, tipo_evento;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.1-c (%sql): prueba del cruce con algo que ya sabes ──
# MAGIC -- en una portabilidad de entrada la línea encontrada debe NACER con la ejecución (cap. 4: el STA es la ejecución)
# MAGIC SELECT count(*) AS portabilidades_in, count(p.sub_id) AS con_linea,
# MAGIC        count_if(abs(timestampdiff(HOUR, p.fecha_ts, a.fecha_ts)) <= 24) AS linea_nace_a_24h,
# MAGIC        round(100 * count_if(abs(timestampdiff(HOUR, p.fecha_ts, a.fecha_ts)) <= 24) / count(*), 1) AS pct
# MAGIC FROM gold.linea_tiempo p
# MAGIC LEFT JOIN gold.linea_tiempo a
# MAGIC   ON a.tipo_evento = 'ALTA' AND a.isdn9 = p.isdn9 AND a.sub_id = p.sub_id AND a.tipo_linea = p.tipo_linea
# MAGIC WHERE p.tipo_evento = 'PORTABILIDAD_IN';

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
# MAGIC -- ── CELDA E8.2-1 (%sql): la tabla y el conteo por regla ──
# MAGIC CREATE OR REPLACE TABLE gold.inconsistencias
# MAGIC COMMENT 'Contradicciones entre fuentes de la linea de tiempo: una fila por caso'
# MAGIC AS
# MAGIC WITH lt AS (SELECT * FROM gold.linea_tiempo),
# MAGIC automaticas AS (
# MAGIC   -- un sub_id puede estar en las dos tablas: una fila por sub_id para no duplicar bajas
# MAGIC   SELECT sub_id, concat_ws('+', sort_array(collect_set(origen))) AS origen
# MAGIC   FROM (SELECT SUB_ID AS sub_id, 'auto_terminate_sub' AS origen FROM bronze.cm_pos__auto_terminate_sub
# MAGIC         UNION ALL
# MAGIC         SELECT SUB_ID, 'auto_cancel_sub' FROM bronze.cm_pos__auto_cancel_sub)
# MAGIC   GROUP BY sub_id),
# MAGIC r1 AS (
# MAGIC   -- solo pospago: en prepago casi todas las bajas las hace el sistema (vencimiento, kits) y no llevan solicitud.
# MAGIC   -- no cuentan las que cierran una migración, un cambio de titular o de número: esas bajas son esperadas
# MAGIC   SELECT 'R1 baja pospago sin solicitud ni portabilidad en ±3 dias' AS regla, b.isdn9, b.sub_id, b.fecha_ts,
# MAGIC          concat_ws('; ', b.detalle, 'explicacion=' || coalesce(a.origen, 'ninguna')) AS detalle
# MAGIC   FROM (SELECT * FROM lt WHERE tipo_evento = 'BAJA' AND tipo_linea = 'POS') b
# MAGIC   LEFT ANTI JOIN lt e
# MAGIC     ON e.isdn9 = b.isdn9
# MAGIC    AND e.tipo_evento IN ('SOLICITUD_BAJA', 'PORTABILIDAD_OUT', 'MIGRACION', 'CAMBIO_TITULAR', 'CAMBIO_NUMERO')
# MAGIC    AND abs(datediff(e.fecha_ts, b.fecha_ts)) <= 3
# MAGIC   LEFT JOIN automaticas a ON a.sub_id = b.sub_id),
# MAGIC r2 AS (
# MAGIC   SELECT 'R2 portabilidad de salida sin baja despues' AS regla, p.isdn9, p.sub_id, p.fecha_ts, p.detalle
# MAGIC   FROM (SELECT * FROM lt WHERE tipo_evento = 'PORTABILIDAD_OUT') p
# MAGIC   LEFT ANTI JOIN (SELECT * FROM lt WHERE tipo_evento = 'BAJA') b
# MAGIC     ON b.isdn9 = p.isdn9 AND b.fecha_ts >= p.fecha_ts - INTERVAL 1 DAY),
# MAGIC r3 AS (
# MAGIC   -- la línea CEDIDA normalmente queda STATUS 0 sin baja; lo raro es que ya tuviera baja antes del cambio
# MAGIC   SELECT 'R3 cambio de titular sobre una linea ya dada de baja' AS regla,
# MAGIC          l.isdn9, l.sub_id, silver.f_ts(c.CREATE_TIME) AS fecha_ts,
# MAGIC          concat_ws('; ', 'baja=' || l.baja_ts, 'STATUS=' || l.status, 'paso a sub ' || c.TO_SUB_ID) AS detalle
# MAGIC   FROM bronze.cm_pos__change_owner_log c
# MAGIC   JOIN silver.lineas l
# MAGIC     ON l.sub_id = c.FROM_SUB_ID AND l.tipo = CASE c.DB_TYPE WHEN '0' THEN 'POS' WHEN '1' THEN 'PRE' END
# MAGIC   WHERE l.baja_ts < silver.f_ts(c.CREATE_TIME) - INTERVAL 1 DAY),
# MAGIC primera_alta AS (
# MAGIC   SELECT isdn9, min(fecha_ts) AS alta_min FROM lt WHERE tipo_evento = 'ALTA' GROUP BY isdn9),
# MAGIC r4 AS (
# MAGIC   SELECT 'R4 evento antes del primer alta del numero' AS regla, e.isdn9, e.sub_id, e.fecha_ts,
# MAGIC          concat_ws('; ', e.tipo_evento, e.fuente, 'alta=' || a.alta_min) AS detalle
# MAGIC   FROM lt e JOIN primera_alta a ON a.isdn9 = e.isdn9
# MAGIC   WHERE e.tipo_evento <> 'ALTA' AND e.fecha_ts < a.alta_min - INTERVAL 1 DAY),
# MAGIC vivas AS (
# MAGIC   -- activas sin baja; en prepago sin los kits (ACT_STATUS con 3). Incluye las bloqueadas, no solo 00/000
# MAGIC   SELECT tipo, sub_id, isdn9, mig_ts, alta_ts, act_status FROM silver.lineas
# MAGIC   WHERE status = '2' AND baja_ts IS NULL AND isdn9 IS NOT NULL AND isdn9 <> ''
# MAGIC     AND (tipo = 'POS' OR coalesce(act_status, '') NOT LIKE '%3%')),
# MAGIC tramite AS (
# MAGIC   -- por número: el NEW_SUB_ID de CM_POS.TRANSFER_REQUEST no sirve como llave (cap. 4)
# MAGIC   SELECT isdn9, bool_or(sentido = 'PRE→POS') AS hubo_pre_a_pos, bool_or(sentido = 'POS→PRE') AS hubo_pos_a_pre
# MAGIC   FROM silver.tramites_migracion
# MAGIC   WHERE isdn9 IS NOT NULL
# MAGIC   GROUP BY isdn9),
# MAGIC r5 AS (
# MAGIC   SELECT 'R5 numero activo en prepago y pospago a la vez' AS regla, po.isdn9, po.sub_id,
# MAGIC          coalesce(po.mig_ts, po.alta_ts) AS fecha_ts,
# MAGIC          concat_ws('; ', 'sub_pre=' || pr.sub_id, 'act=' || pr.act_status || '/' || po.act_status,
# MAGIC                    'causa=' || CASE
# MAGIC                      WHEN po.mig_ts IS NOT NULL OR t.hubo_pre_a_pos THEN 'migracion PRE->POS que no cerro el prepago'
# MAGIC                      WHEN pr.mig_ts IS NOT NULL OR t.hubo_pos_a_pre THEN 'migracion POS->PRE que no cerro el pospago'
# MAGIC                      ELSE 'sin rastro de migracion' END) AS detalle
# MAGIC   FROM vivas po
# MAGIC   JOIN vivas pr ON pr.isdn9 = po.isdn9 AND pr.tipo = 'PRE'
# MAGIC   LEFT JOIN tramite t ON t.isdn9 = po.isdn9
# MAGIC   WHERE po.tipo = 'POS')
# MAGIC SELECT * FROM r1 UNION ALL SELECT * FROM r2 UNION ALL SELECT * FROM r3 UNION ALL SELECT * FROM r4
# MAGIC UNION ALL SELECT * FROM r5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- conteo por regla
# MAGIC SELECT regla, count(*) AS casos, count(DISTINCT isdn9) AS numeros
# MAGIC FROM gold.inconsistencias GROUP BY regla ORDER BY regla;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.2-2 (%sql) ──
# MAGIC -- R1 partido por explicación (¿cuántas son bajas automáticas?)
# MAGIC SELECT regexp_extract(detalle, 'explicacion=([a-z_+]+)', 1) AS explicacion, count(*) AS bajas
# MAGIC FROM gold.inconsistencias WHERE regla LIKE 'R1%' GROUP BY 1 ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.2-3 (%sql) ──
# MAGIC -- R5 por causa probable
# MAGIC SELECT regexp_extract(detalle, 'causa=(.*)$', 1) AS causa, count(DISTINCT isdn9) AS numeros
# MAGIC FROM gold.inconsistencias WHERE regla LIKE 'R5%' GROUP BY 1 ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.2-4 (%sql) ──
# MAGIC -- R5 comparable con las 544 que se reportaron a ITOps (solo ACT_STATUS 00 en prepago y 000 en pospago)
# MAGIC SELECT count(DISTINCT isdn9) AS numeros_como_el_reporte
# MAGIC FROM gold.inconsistencias WHERE regla LIKE 'R5%' AND detalle LIKE '%act=00/000;%';

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

# CELDA E8.3-1 (Python, sin %sql) ──
# E8.3 · a) columnas nuevas en el diccionario (solo si aún no existen: así la celda se puede repetir)
cols = [c.lower() for c in spark.table("gold.diccionario_columnas").columns]
if "significado_datos" not in cols:
    spark.sql("""ALTER TABLE gold.diccionario_columnas
                 ADD COLUMNS (significado_datos STRING, confiabilidad STRING, fuente_recomendada STRING)""")
print(spark.table("gold.diccionario_columnas").columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.3-2 (%sql) ──
# MAGIC -- E8.3 · b) lo que aprendiste, con el % que lo respalda
# MAGIC MERGE INTO gold.diccionario_columnas d
# MAGIC USING (SELECT * FROM VALUES
# MAGIC   ('cm_pos__sub_mb', 'STA_DATETIME', 'Alta del periodo; se hereda en la migracion PRE a POS y en el cambio de titular',
# MAGIC    'Heredado en 28,22 % de las POS (gemelo PRE, E4.3); en portabilidad es la ejecucion (E4.6)', 'gold.linea_tiempo (ALTA)'),
# MAGIC   ('cm_pos__sub_mb', 'MIG_DATETIME', 'Inicio real del periodo migrado PRE a POS',
# MAGIC    '100 % a 1 dia o menos de TRANSFER_REQUEST; 99,1-100 % a 15 min del CRM (E4.5)', 'MIG_DATETIME'),
# MAGIC   ('cm_pos__sub_mb', 'END_DATETIME', 'Baja del periodo; NULL con STATUS 0 = linea cedida en cambio de titular',
# MAGIC    'POS 0 sin baja: 95,4 % estan en CHANGE_OWNER_LOG (cierre cap. 4)', 'silver.lineas.baja_ts + CHANGE_OWNER_LOG'),
# MAGIC   ('cm_pos__sub_mb', 'STATUS', 'Estado: 0 paso a prepago (con baja) o cedida (sin baja); 4 cambio de titular antiguo; 9 preactivada',
# MAGIC    'Confianza por valor en gold.catalogo_valores', 'silver.f_estado_linea(tipo, status, act_status, con_baja)'),
# MAGIC   ('cm_pos__sub_mb', 'ACT_STATUS', 'Bloqueos, un digito por causa: cliente, deuda, limite de credito',
# MAGIC    'Catalogo oficial AP_DOMAIN TYPE 306', 'silver.f_bloqueo'),
# MAGIC   ('cm_pre__sub_mb', 'STA_DATETIME', 'Alta del periodo; se hereda en la migracion POS a PRE',
# MAGIC    'Altas nuevas: el 00 calza a 15 min en 96,1 % (E4.6)', 'gold.linea_tiempo (ALTA)'),
# MAGIC   ('cm_pre__sub_mb', 'MIG_DATETIME', 'Inicio real del periodo migrado POS a PRE',
# MAGIC    '100 % a 1 dia o menos de CM_PRE.TRANSFER_REQUEST (E4.5)', 'MIG_DATETIME'),
# MAGIC   ('cm_pre__sub_mb', 'END_DATETIME', 'Baja del periodo; NULL con STATUS 0 = cedida, o kit borrado si ACT_STATUS es 03',
# MAGIC    'PRE 0 sin baja y sin kit: 99,4 % cedidas (cierre cap. 4)', 'silver.lineas.baja_ts'),
# MAGIC   ('cm_pre__sub_mb', 'STATUS', 'Estado: 2 incluye kits; 0 cedida o eliminada; 4 kits cancelados en 2019',
# MAGIC    'Confianza por valor en gold.catalogo_valores', 'silver.f_estado_linea(tipo, status, act_status, con_baja)'),
# MAGIC   ('cm_pre__sub_mb', 'ACT_STATUS', 'Bloqueos: cliente, sistema; 3 kit, 4 sin saldo',
# MAGIC    'AP_DOMAIN TYPE 02 + CM_PRE TYPE 100', 'silver.f_bloqueo'),
# MAGIC   ('cm_pos__transfer_request', 'NEW_SUB_ID', 'No es el SUB_ID del pospago nuevo',
# MAGIC    'Solo 18,4 % es el SUB_ID POS del numero (0,7 % en 2026)', 'ISDN + fecha'),
# MAGIC   ('cm_pos__change_owner_log', 'FROM_SUB_ID', 'Linea cedida en el cambio de titular',
# MAGIC    '95,4 % POS y 99,4 % PRE quedan STATUS 0 sin baja', 'gold.linea_tiempo (CAMBIO_TITULAR)'),
# MAGIC   ('mnp__porting_request', 'PORT_NUMBER', 'Numero portado. La tabla no trae SUB_ID ni ISDN',
# MAGIC    'Cruce por numero + EXECUTION_DATE: % encontrado en E8.1-b y E8.1-c', 'silver.tenencia (numero + fecha)'),
# MAGIC   ('cm_pre__termination_sub', 'ISDN', 'Numero de la baja prepago (IMPORT_DATETIME pedida, TERMINATE_TIME ejecutada). Sin SUB_ID',
# MAGIC    'Cruce por numero + fecha: % encontrado en E8.1-b', 'silver.tenencia (numero + fecha)'),
# MAGIC   ('cm_pre__sub_sim_transfer', 'ISDN', 'Numero de cada SIM que tuvo (CHANGE_DATETIME a END_DATETIME). Sin SUB_ID',
# MAGIC    'Cruce por numero + fecha: % encontrado en E8.1-b', 'silver.tenencia (numero + fecha)'),
# MAGIC   ('cm_pre__status', 'NEW_STATUS', 'Estado nuevo de un numero prepago (log de cambios)',
# MAGIC    'Muy repetitivo: 91.949 de 93.886 eventos repiten el estado (E6.4)', 'gold.estado_linea_pre_scd2')
# MAGIC   AS v(table_name, column_name, significado_datos, confiabilidad, fuente_recomendada)) o
# MAGIC ON d.table_schema = 'bronze' AND d.table_name = o.table_name AND upper(d.column_name) = o.column_name
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   d.significado_datos = o.significado_datos, d.confiabilidad = o.confiabilidad,
# MAGIC   d.fuente_recomendada = o.fuente_recomendada;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- terminado cuando: las 16 filas quedaron llenas
# MAGIC SELECT table_name, column_name, significado_datos, confiabilidad, fuente_recomendada
# MAGIC FROM gold.diccionario_columnas
# MAGIC WHERE significado_datos IS NOT NULL
# MAGIC ORDER BY table_name, column_name;

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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.4-1 (%sql) ──
# MAGIC -- 1) migraciones por mes, con y sin trámite (cruce por número y fecha: NEW_SUB_ID no sirve como llave)
# MAGIC WITH mig AS (SELECT * FROM silver.transiciones WHERE tipo_ant <> tipo AND mismo_cliente),
# MAGIC con_tramite AS (
# MAGIC   SELECT DISTINCT m.isdn9, m.sub_id
# MAGIC   FROM mig m
# MAGIC   JOIN silver.tramites_migracion r
# MAGIC     ON r.isdn9 = m.isdn9 AND r.sentido = m.transicion
# MAGIC    AND abs(datediff(m.inicio_ts, r.tramite_ts)) <= 3)
# MAGIC SELECT date_trunc('month', m.inicio_ts) AS mes, m.transicion,
# MAGIC        count(*) AS migraciones, count(c.sub_id) AS con_tramite, count(*) - count(c.sub_id) AS sin_tramite
# MAGIC FROM mig m
# MAGIC LEFT JOIN con_tramite c ON c.isdn9 = m.isdn9 AND c.sub_id = m.sub_id
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.4-2 (%sql) ──
# MAGIC -- 2) % de altas pospago con STA heredado (debe dar el 28,22 % de E4.3)
# MAGIC SELECT round(100 * count_if(g.sub_pos IS NOT NULL) / count(*), 2) AS pct_pos_sta_heredado
# MAGIC FROM (SELECT sub_id FROM silver.lineas WHERE tipo = 'POS') l
# MAGIC LEFT JOIN (SELECT DISTINCT sub_pos FROM silver.gemelos) g ON g.sub_pos = l.sub_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.4-3 (%sql) ──
# MAGIC -- 3) días de solicitud a ejecución de una portabilidad (mismos estados que en E8.1)
# MAGIC SELECT TYPE, count(*) AS portabilidades,
# MAGIC        round(avg(timestampdiff(HOUR, silver.f_ts(CREATE_DATE), silver.f_ts(EXECUTION_DATE))) / 24, 2) AS prom_dias,
# MAGIC        percentile_approx(timestampdiff(HOUR, silver.f_ts(CREATE_DATE), silver.f_ts(EXECUTION_DATE)) / 24, 0.5) AS mediana_dias
# MAGIC FROM bronze.mnp__porting_request
# MAGIC WHERE STATE_CODE IN ('00A02', '01A06')
# MAGIC   AND silver.f_ts(EXECUTION_DATE) >= silver.f_ts(CREATE_DATE)
# MAGIC GROUP BY TYPE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.4-4 (%sql) ──
# MAGIC -- 4) números reciclados (otro cliente, línea anterior dada de baja) y tiempo libre; sin los cambios de titular
# MAGIC SELECT count(DISTINCT t.isdn9) AS numeros_reciclados, count(*) AS reciclajes,
# MAGIC        round(avg(t.dias), 1) AS prom_dias_libre, percentile_approx(t.dias, 0.5) AS mediana_dias_libre
# MAGIC FROM silver.transiciones t
# MAGIC LEFT ANTI JOIN (SELECT DISTINCT FROM_SUB_ID AS sub_id, CASE DB_TYPE WHEN '0' THEN 'POS' WHEN '1' THEN 'PRE' END AS tipo
# MAGIC                 FROM bronze.cm_pos__change_owner_log) c
# MAGIC   ON c.sub_id = t.sub_ant AND c.tipo = t.tipo_ant
# MAGIC WHERE NOT t.mismo_cliente AND t.baja_ant IS NOT NULL AND t.dias >= 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── CELDA E8.4-5 (%sql) ──
# MAGIC -- 5) cambio de titular y baja de la línea nueva en los 90 días siguientes (solo cambios con 90 días de observación)
# MAGIC WITH corte AS (
# MAGIC   SELECT max(fecha_ts) AS hasta FROM gold.linea_tiempo
# MAGIC   WHERE tipo_evento IN ('ALTA', 'BAJA') AND fecha_ts <= current_timestamp()),     -- ignora fechas futuras raras
# MAGIC ct AS (SELECT l.* FROM gold.linea_tiempo l, corte
# MAGIC        WHERE l.tipo_evento = 'CAMBIO_TITULAR' AND l.fecha_ts <= corte.hasta - INTERVAL 90 DAYS),
# MAGIC b AS (SELECT * FROM gold.linea_tiempo WHERE tipo_evento = 'BAJA')
# MAGIC SELECT count(DISTINCT ct.sub_id) AS lineas_con_cambio,
# MAGIC        count(DISTINCT CASE WHEN b.sub_id IS NOT NULL THEN ct.sub_id END) AS baja_en_90_dias
# MAGIC FROM ct
# MAGIC LEFT JOIN b
# MAGIC   ON b.sub_id = ct.sub_id AND b.isdn9 = ct.isdn9
# MAGIC  AND b.fecha_ts BETWEEN ct.fecha_ts AND ct.fecha_ts + INTERVAL 90 DAYS;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.diccionario_columnas
# MAGIC SET significado_datos = 'Numero portado o retornado (TYPE 01/05 portabilidad, 02/06 retorno). Sin SUB_ID ni ISDN',
# MAGIC     confiabilidad = 'Salida: 99,99 % encuentra su linea; entrada: 91,1 % (89,2 % nace a 24 h). BYPASS sin fecha de ejecucion',
# MAGIC     fuente_recomendada = 'gold.linea_tiempo (PORTABILIDAD_*, RETORNO_*)'
# MAGIC WHERE table_name = 'mnp__porting_request' AND column_name = 'PORT_NUMBER';
# MAGIC
# MAGIC UPDATE gold.diccionario_columnas
# MAGIC SET significado_datos = 'Baja prepago YA hecha, no solicitud. IMPORT_DATETIME registro, TERMINATE_TIME ejecucion',
# MAGIC     confiabilidad = '99,5 % encuentra su linea; 98,8 % cae justo despues del fin de la linea (<= 3 dias)',
# MAGIC     fuente_recomendada = 'silver.lineas.baja_ts (fecha) + esta tabla (motivo)'
# MAGIC WHERE table_name = 'cm_pre__termination_sub' AND column_name = 'ISDN';
# MAGIC
# MAGIC UPDATE gold.diccionario_columnas
# MAGIC SET significado_datos = 'SIM asignada al numero prepago; 97,6 % es la SIM del alta, no un cambio',
# MAGIC     confiabilidad = '98,9 % encuentra su linea; cambios reales ~78 mil',
# MAGIC     fuente_recomendada = 'gold.linea_tiempo CAMBIO_SIM sin "SIM del alta"'
# MAGIC WHERE table_name = 'cm_pre__sub_sim_transfer' AND column_name = 'ISDN';