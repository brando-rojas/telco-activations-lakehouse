# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 · Capítulo 3: entidades relacionales
# MAGIC
# MAGIC **Dominios del examen:** Development & Ingestion · Data Processing · Governance (comentarios, information_schema).
# MAGIC **Tiempo:** 2 h. **Lee antes:** cap. 3 (catálogos y esquemas, tablas administradas y externas, vistas, vistas temporales, CTAS, `INSERT OVERWRITE`, `INSERT INTO`, `MERGE`).
# MAGIC
# MAGIC Aquí cargas **todo** bronze con un solo bucle parametrizado y armas el primer diccionario de datos.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.1 · Cargar todo bronze con un bucle
# MAGIC **Tareas:**
# MAGIC 1. Completa la función `cargar_bronze(nombre)`:
# MAGIC    - tablas del núcleo: todos sus parquet;
# MAGIC    - logs: solo los `DIAS_LOGS` archivos más recientes (cada archivo es un día: `TABLA_YYYYMMDD.parquet`).
# MAGIC 2. Agrega `_archivo` y `_ingesta`.
# MAGIC 3. Escribe en modo `overwrite` con `overwriteSchema`.
# MAGIC 4. Corre el bucle y guarda un resumen (tabla, filas, segundos).
# MAGIC
# MAGIC **Terminado cuando:** todas las tablas presentes en el volumen tienen su tabla bronze y el resumen no tiene errores.

# COMMAND ----------

import time

def cargar_bronze(nombre):
    fs = archivos_parquet(nombre)
    if not fs:
        return None
    if nombre in LOGS and DIAS_LOGS:
        fs = fs[-DIAS_LOGS:]          # los nombres terminan en _YYYYMMDD: orden alfabético = orden de fechas
    df = (spark.read.parquet(*fs)
          .withColumn("_archivo", F.col("_metadata.file_path"))
          .withColumn("_ingesta", F.current_timestamp()))
    # TODO: escribe df en tb(nombre) con mode("overwrite") y option("overwriteSchema", "true")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tb(nombre))
    return spark.table(tb(nombre)).count()

resumen = []
for nombre in NUCLEO + LOGS:
    t0 = time.time()
    try:
        n = cargar_bronze(nombre)
        resumen.append((nombre, tb(nombre), n, round(time.time() - t0, 1), None if n is not None else "sin archivos"))
    except Exception as e:
        resumen.append((nombre, tb(nombre), None, round(time.time() - t0, 1), str(e)[:200]))
display(spark.createDataFrame(resumen, "origen string, tabla string, filas long, segundos double, error string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.2 · Administradas y externas
# MAGIC **Tarea:** corre `DESCRIBE EXTENDED` sobre una tabla bronze y anota `Type` y `Location`.
# MAGIC **Responde:** si haces `DROP TABLE` de una tabla administrada, ¿qué pasa con sus archivos? ¿Y en una externa? ¿Por qué en Free Edition no puedes crear externas? *(No hay external locations.)*

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bronze.cm_pos__sub_mb

# COMMAND ----------

# MAGIC %md
# MAGIC - **DROP de una administrada:** se borran los metadatos y Unity Catalog borra también los archivos (pasados unos días; mientras tanto UNDROP TABLE la recupera).
# MAGIC - **DROP de una externa:** solo se borran los metadatos; los archivos quedan en su ubicación.
# MAGIC - **¿Por qué no hay externas en Free Edition?** Una tabla externa necesita una *external location* (ruta de nube + credencial de almacenamiento) y Free Edition no permite ubicaciones de almacenamiento propias: todo vive en el almacenamiento administrado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.3 · Vistas: persistentes y temporales
# MAGIC **Tareas:**
# MAGIC 1. Crea la vista **persistente** `silver.v_lineas_activas` con las líneas STATUS = '2' de prepago y pospago (`UNION ALL`, con una columna `tipo`).
# MAGIC 2. Crea una vista **temporal** `tmp_lineas_canceladas` (STATUS = '3').
# MAGIC 3. Abre otro notebook y prueba consultar las dos. ¿Cuál existe y cuál no?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE VIEW silver.v_lineas_activas AS ...
# MAGIC CREATE OR REPLACE VIEW silver.v_lineas_activas
# MAGIC COMMENT 'Lineas activas (STATUS 2) de prepago y pospago. En pospago el cliente sale del contrato.'
# MAGIC AS
# MAGIC SELECT 'PRE' AS tipo, s.SUB_ID, s.ISDN, s.CUST_ID, CAST(NULL AS STRING) AS CONTRACT_ID,
# MAGIC        s.STA_DATETIME, s.STATUS, s.ACT_STATUS
# MAGIC FROM bronze.cm_pre__sub_mb s
# MAGIC WHERE s.STATUS = '2'
# MAGIC UNION ALL
# MAGIC SELECT 'POS', s.SUB_ID, s.ISDN, c.CUST_ID, s.CONTRACT_ID,
# MAGIC        s.STA_DATETIME, s.STATUS, s.ACT_STATUS
# MAGIC FROM bronze.cm_pos__sub_mb s
# MAGIC LEFT JOIN (SELECT CONTRACT_ID, max(CUST_ID) AS CUST_ID          -- un contrato = una fila
# MAGIC            FROM bronze.cm_pos__contract GROUP BY CONTRACT_ID) c
# MAGIC        ON c.CONTRACT_ID = s.CONTRACT_ID
# MAGIC WHERE s.STATUS = '2';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE TEMP VIEW tmp_lineas_canceladas AS ...
# MAGIC CREATE OR REPLACE TEMP VIEW tmp_lineas_canceladas AS
# MAGIC SELECT 'PRE' AS tipo, SUB_ID, ISDN, STA_DATETIME, END_DATETIME FROM bronze.cm_pre__sub_mb WHERE STATUS = '3'
# MAGIC UNION ALL
# MAGIC SELECT 'POS', SUB_ID, ISDN, STA_DATETIME, END_DATETIME FROM bronze.cm_pos__sub_mb WHERE STATUS = '3';

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.4 · Decodificar estados con AP_DOMAIN
# MAGIC AP_DOMAIN es el catálogo de dominios: para cada `TYPE` (qué campo es), los códigos y su significado.
# MAGIC **Tareas:**
# MAGIC 1. Explora `bronze.cm_pre__ap_domain` y `bronze.cm_pos__ap_domain`: ¿qué `TYPE` corresponden a STATUS y ACT_STATUS de SUB_MB?
# MAGIC 2. Crea `silver.v_dominio` con (owner, type, codigo, significado) de los dos esquemas.
# MAGIC 3. Cuenta las líneas prepago por STATUS con su significado.
# MAGIC
# MAGIC **Terminado cuando:** cada STATUS de prepago tiene nombre. Si alguno no aparece en AP_DOMAIN, anótalo como hallazgo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.cm_pos__ap_domain ORDER BY 1 --LIMIT 200

# COMMAND ----------

# MAGIC %sql
# MAGIC -- E3.4 paso 1 · qué TYPE hay en cada AP_DOMAIN (TYPE y CODE son texto y a veces traen espacios: trim)
# MAGIC SELECT 'CM_PRE' AS owner, trim(TYPE) AS type, count(*) AS codigos,
# MAGIC        slice(array_sort(collect_set(trim(CODE))), 1, 8) AS codigos_ej,
# MAGIC        slice(array_sort(collect_set(trim(NAME))), 1, 4) AS nombres_ej
# MAGIC FROM bronze.cm_pre__ap_domain GROUP BY trim(TYPE)
# MAGIC UNION ALL
# MAGIC SELECT 'CM_POS', trim(TYPE), count(*),
# MAGIC        slice(array_sort(collect_set(trim(CODE))), 1, 8), slice(array_sort(collect_set(trim(NAME))), 1, 4)
# MAGIC FROM bronze.cm_pos__ap_domain GROUP BY trim(TYPE)
# MAGIC ORDER BY owner, codigos DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- pista: qué TYPE tiene códigos que calzan con los valores reales de STATUS y de ACT_STATUS
# MAGIC -- (que los códigos calcen no basta: lee los nombres; '301' calza con STATUS y son tipos de documento)
# MAGIC WITH valores AS (
# MAGIC   SELECT 'STATUS' AS campo, STATUS AS valor FROM bronze.cm_pre__sub_mb
# MAGIC   UNION SELECT 'STATUS', STATUS FROM bronze.cm_pos__sub_mb
# MAGIC   UNION SELECT 'ACT_STATUS', ACT_STATUS FROM bronze.cm_pre__sub_mb
# MAGIC   UNION SELECT 'ACT_STATUS', ACT_STATUS FROM bronze.cm_pos__sub_mb),
# MAGIC tot AS (SELECT campo, count(DISTINCT valor) AS valores_existentes FROM valores GROUP BY campo),
# MAGIC dom AS (
# MAGIC   SELECT 'CM_PRE' AS owner, trim(TYPE) AS type, trim(CODE) AS codigo FROM bronze.cm_pre__ap_domain
# MAGIC   UNION ALL
# MAGIC   SELECT 'CM_POS', trim(TYPE), trim(CODE) FROM bronze.cm_pos__ap_domain)
# MAGIC SELECT v.campo, d.owner, d.type, count(DISTINCT v.valor) AS valores_que_calzan, t.valores_existentes
# MAGIC FROM valores v
# MAGIC JOIN dom d ON d.codigo = v.valor
# MAGIC JOIN tot t ON t.campo = v.campo
# MAGIC GROUP BY v.campo, d.owner, d.type, t.valores_existentes
# MAGIC ORDER BY v.campo, valores_que_calzan DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE VIEW silver.v_dominio AS ...
# MAGIC CREATE OR REPLACE VIEW silver.v_dominio
# MAGIC COMMENT 'AP_DOMAIN de CM_PRE y CM_POS: owner, type, codigo y significado. El mismo TYPE puede significar distinto en cada esquema'
# MAGIC AS
# MAGIC SELECT 'CM_PRE' AS owner, trim(TYPE) AS type, trim(CODE) AS codigo, trim(NAME) AS significado FROM bronze.cm_pre__ap_domain
# MAGIC UNION ALL
# MAGIC SELECT 'CM_POS', trim(TYPE), trim(CODE), trim(NAME) FROM bronze.cm_pos__ap_domain;

# COMMAND ----------

# MAGIC %md
# MAGIC **Hallazgo E3.4:** AP_DOMAIN es un catálogo de parámetros del CRM, no de estados.
# MAGIC - TYPE `301` de **CM_POS** = tipo de documento (`ID_TYPE` de `CM_POS.CUSTOMER`). Vale para clientes prepago y pospago.
# MAGIC - El `301` de CM_PRE es otro catálogo (en inglés) y no se usa.
# MAGIC - El `STATUS` de SUB_MB no se decodifica en AP_DOMAIN: 2 = activa, 3 = cancelada; 0, 1, 4, 9 por confirmar.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3a · líneas prepago por STATUS. STATUS no tiene TYPE en AP_DOMAIN (es el hallazgo): su significado lo da E3.6
# MAGIC SELECT STATUS, count(*) AS lineas
# MAGIC FROM bronze.cm_pre__sub_mb
# MAGIC GROUP BY STATUS
# MAGIC ORDER BY lineas DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3b · ACT_STATUS de prepago con su nombre oficial (CM_POS TYPE '02'). El 03 y el 04 no están: los explica E3.6
# MAGIC SELECT s.ACT_STATUS, d.significado, count(*) AS lineas
# MAGIC FROM bronze.cm_pre__sub_mb s
# MAGIC LEFT JOIN silver.v_dominio d ON d.owner = 'CM_POS' AND d.type = '02' AND d.codigo = s.ACT_STATUS
# MAGIC GROUP BY s.ACT_STATUS, d.significado
# MAGIC ORDER BY lineas DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3c · lo que sí decodifica el TYPE '301': el tipo de documento del cliente, con la definición de CM_POS para los dos
# MAGIC --      (en CM_PRE el 301 es una plantilla vieja: carnet, licencia de conducir...)
# MAGIC SELECT c.ID_TYPE, d.significado, count(*) AS clientes
# MAGIC FROM bronze.cm_pos__customer c
# MAGIC LEFT JOIN silver.v_dominio d ON d.owner = 'CM_POS' AND d.type = '301' AND d.codigo = c.ID_TYPE
# MAGIC GROUP BY c.ID_TYPE, d.significado
# MAGIC ORDER BY clientes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.5 · CTAS, INSERT OVERWRITE, INSERT INTO y MERGE
# MAGIC Con una tabla chica ves en `DESCRIBE HISTORY` qué operación deja cada comando.
# MAGIC **Tareas, en orden, sobre `gold.motivos`** (de `bronze.cm_pos__reason`):
# MAGIC 1. CTAS con los motivos.
# MAGIC 2. `INSERT OVERWRITE` con solo los primeros 10 motivos.
# MAGIC 3. `INSERT INTO` con los mismos 10 (quedan duplicados).
# MAGIC 4. `MERGE` que inserte los que faltan y no duplique.
# MAGIC 5. `DESCRIBE HISTORY gold.motivos`.
# MAGIC
# MAGIC **Responde:** ¿cuál de los cuatro es idempotente si lo corres dos veces? ¿Cuál cambia el esquema si la consulta trae una columna nueva?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: los 5 pasos
# MAGIC DESCRIBE bronze.cm_pos__reason

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) CTAS
# MAGIC CREATE OR REPLACE TABLE gold.motivos AS
# MAGIC SELECT REASON_ID, CODE, NAME, TYPE FROM bronze.cm_pos__reason;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) INSERT OVERWRITE con 10 motivos (mismo esquema; reemplaza los datos)
# MAGIC INSERT OVERWRITE gold.motivos
# MAGIC SELECT REASON_ID, CODE, NAME, TYPE FROM bronze.cm_pos__reason ORDER BY REASON_ID LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) INSERT INTO con los mismos 10 (se duplican)
# MAGIC INSERT INTO gold.motivos
# MAGIC SELECT REASON_ID, CODE, NAME, TYPE FROM bronze.cm_pos__reason ORDER BY REASON_ID LIMIT 10;
# MAGIC SELECT count(*) AS filas, count(DISTINCT REASON_ID) AS motivos FROM gold.motivos;   -- 20 y 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4) MERGE: inserta solo los que faltan
# MAGIC MERGE INTO gold.motivos d
# MAGIC USING (SELECT REASON_ID, CODE, NAME, TYPE FROM bronze.cm_pos__reason) o
# MAGIC ON d.REASON_ID = o.REASON_ID
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC SELECT count(*) AS filas, count(DISTINCT REASON_ID) AS motivos FROM gold.motivos;   -- los 10 duplicados siguen

# COMMAND ----------

# MAGIC %sql
# MAGIC -- (opcional) limpiar los duplicados del paso 3 y volver a correr el MERGE: ya no inserta nada
# MAGIC INSERT OVERWRITE gold.motivos SELECT DISTINCT * FROM gold.motivos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5) historial
# MAGIC DESCRIBE HISTORY gold.motivos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E3.6 · Diccionario de datos con information_schema y comentarios
# MAGIC **Tareas:**
# MAGIC 1. Consulta `information_schema.columns` del esquema `bronze`: tabla, columna, tipo.
# MAGIC 2. Agrega comentarios a SUB_MB (prepago y pospago) con lo que **ya sabes** de las columnas:
# MAGIC    STATUS (2 activa, 3 cancelada, 1 registrada, 0 inactiva), ACT_STATUS (contiene 1 = bloqueo 1 vía, contiene 2 = bloqueo 2 vías),
# MAGIC    STA_DATETIME (alta; **se hereda en la migración**), END_DATETIME, MIG_DATETIME.
# MAGIC 3. Guarda el diccionario en `gold.diccionario_columnas` (incluye la columna `comment`).
# MAGIC
# MAGIC **Terminado cuando:** `gold.diccionario_columnas` tiene los comentarios de SUB_MB. Lo completas al final del cap. 4 con lo que descubras.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, column_name, data_type, comment
# MAGIC FROM workspace.information_schema.columns
# MAGIC WHERE table_schema = 'bronze'
# MAGIC ORDER BY table_name, ordinal_position;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solo PRE, 2.º dígito: 3 = entrantes bloqueadas (BI_REPORT: preactivada/kit), 4 = bloqueada por falta de saldo.
# MAGIC CREATE OR REPLACE FUNCTION silver.f_bloqueo(origen STRING, act_status STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'ACT_STATUS en palabras. 1.er digito = cliente, 2.o = deuda (POS) o sistema (PRE), 3.o (solo POS) = limite de credito. Fuente: CM_POS.AP_DOMAIN TYPE 306 y 02, CM_PRE.AP_DOMAIN TYPE 100; preactivada (PRE 3) tambien en BI_REPORT'
# MAGIC RETURN CASE
# MAGIC   WHEN act_status IS NULL THEN 'sin dato'
# MAGIC   WHEN NOT act_status RLIKE '^[0-9]{2,3}$' THEN concat('valor no esperado: ', act_status)
# MAGIC   ELSE coalesce(nullif(concat_ws(' + ',
# MAGIC          CASE substr(act_status, 1, 1)
# MAGIC               WHEN '0' THEN NULL
# MAGIC               WHEN '1' THEN 'cliente 1 via'
# MAGIC               WHEN '2' THEN 'cliente 2 vias'
# MAGIC               ELSE concat('1er digito = ', substr(act_status, 1, 1), ' (sin documentar)') END,
# MAGIC          CASE substr(act_status, 2, 1)
# MAGIC               WHEN '0' THEN NULL
# MAGIC               WHEN '1' THEN CASE WHEN origen = 'PRE' THEN 'sistema 1 via' ELSE 'deuda 1 via' END
# MAGIC               WHEN '2' THEN CASE WHEN origen = 'PRE' THEN 'sistema 2 vias' ELSE 'deuda 2 vias' END
# MAGIC               WHEN '3' THEN CASE WHEN origen = 'PRE' THEN 'preactivada (kit sin primer uso; entrantes bloqueadas)'
# MAGIC                                  ELSE '2o digito = 3 (fuera del catalogo)' END
# MAGIC               WHEN '4' THEN CASE WHEN origen = 'PRE' THEN 'bloqueada por falta de saldo'
# MAGIC                                  ELSE '2o digito = 4 (fuera del catalogo)' END
# MAGIC               ELSE concat('2o digito = ', substr(act_status, 2, 1), ' (sin documentar)') END,
# MAGIC          CASE substr(act_status, 3, 1)
# MAGIC               WHEN '' THEN NULL
# MAGIC               WHEN '0' THEN NULL
# MAGIC               WHEN '1' THEN 'limite de credito 1 via'
# MAGIC               WHEN '2' THEN 'limite de credito 2 vias'
# MAGIC               ELSE concat('3er digito = ', substr(act_status, 3, 1), ' (fuera del catalogo)') END
# MAGIC        ), ''), 'sin bloqueo')
# MAGIC END;
# MAGIC  
# MAGIC CREATE OR REPLACE FUNCTION silver.f_estado_linea(origen STRING, status STRING, act_status STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Estado final de la linea combinando STATUS y ACT_STATUS; el mismo numero de STATUS significa cosas distintas en PRE y POS. Detalle en gold.catalogo_valores'
# MAGIC RETURN CASE
# MAGIC   WHEN origen = 'PRE' THEN CASE status
# MAGIC     WHEN '2' THEN CASE WHEN act_status LIKE '%3%' THEN 'PREACTIVADA (kit sin primer uso)'
# MAGIC                        WHEN act_status LIKE '_4'  THEN 'ACTIVA sin saldo (bloqueada)'
# MAGIC                        WHEN act_status LIKE '%2%' THEN 'ACTIVA con bloqueo 2 vias'
# MAGIC                        WHEN act_status LIKE '%1%' THEN 'ACTIVA con bloqueo 1 via'
# MAGIC                        ELSE 'ACTIVA' END
# MAGIC     WHEN '1' THEN 'REGISTRADA SIN CONECTAR'
# MAGIC     WHEN '3' THEN 'CANCELADA'
# MAGIC     WHEN '0' THEN CASE WHEN act_status LIKE '%3%' THEN 'ELIMINADA sin primer uso (kit)'
# MAGIC                        ELSE 'ELIMINADA (cerrada; probable paso a pospago)' END
# MAGIC     WHEN '4' THEN 'CANCELADA EN CIERRE MASIVO'
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
# MAGIC     WHEN '0' THEN 'INACTIVA (paso a prepago)'
# MAGIC     WHEN '4' THEN 'CERRADA POR CAMBIO DE TITULAR'
# MAGIC     ELSE concat('STATUS no esperado: ', coalesce(status, 'NULL')) END
# MAGIC   ELSE 'origen no valido (usa PRE o POS)'
# MAGIC END;
# MAGIC  
# MAGIC

# COMMAND ----------

# Comentarios de SUB_MB. Se aplican solo a las columnas que existen (si una falta, se avisa y se sigue).
comentarios = {
  "bronze.cm_pre__sub_mb": {
    "STATUS": "Estado PRE: 2 activa (si ACT_STATUS tiene 3 es kit preactivado sin primer uso), 3 cancelada, 1 registrada sin conectar (NOT CONNECT), "
              "0 eliminada (BCCS Delete: kit nunca usado o cerrada al pasar a pospago), 4 cancelada en cierre masivo del 13/06/2019, 9 sin significado documentado. "
              "No tiene catalogo en AP_DOMAIN. 0, 4 y 9 NO significan lo mismo que en pospago. Valores: gold.catalogo_valores. "
              "Estado final: silver.f_estado_linea(origen, STATUS, ACT_STATUS)",
    "ACT_STATUS": "Bloqueos PRE, 2 digitos (CM_PRE.AP_DOMAIN TYPE 100 y CM_POS.AP_DOMAIN TYPE 02): 1.o pedido por el cliente, 2.o por sistema; "
                  "0 sin bloqueo, 1 una via (salientes), 2 dos vias. 2.o digito 3 = entrantes bloqueadas, que BI_REPORT lee como preactivada (kit): 03, 13, 23. "
                  "2.o digito 4 = bloqueada por falta de saldo. 00 = normal. Solo describe la linea si STATUS = 2. En palabras: silver.f_bloqueo(origen, ACT_STATUS)",
    "STA_DATETIME": "Alta del periodo. OJO: se hereda en la migracion pre<->pos, no es el inicio real del periodo migrado",
    "END_DATETIME": "Baja o cierre del periodo (NULL si sigue abierto)",
    "MIG_DATETIME": "Fecha de migracion hacia esta linea; BI_REPORT usa NVL(MIG_DATETIME, STA_DATETIME) como inicio",
    "CUST_ID": "Cliente de la linea prepago -> CM_POS.CUSTOMER.CUST_ID",
  },
  "bronze.cm_pos__sub_mb": {
    "STATUS": "Estado POS: 2 activa, 3 cancelada, 1 registrada sin conectar (accion 00 sin 666), 9 preactivada: suscriptor recibido (accion 666) aun sin conexion (00), "
              "0 inactiva: cerrada al pasar a prepago (accion 167), 4 cerrada por cambio de titular (BCCS Changed contract owner). "
              "No tiene catalogo en AP_DOMAIN. 0, 4 y 9 NO significan lo mismo que en prepago. Valores: gold.catalogo_valores. "
              "Estado final: silver.f_estado_linea(origen, STATUS, ACT_STATUS)",
    "ACT_STATUS": "Bloqueos POS, 3 digitos (CM_POS.AP_DOMAIN TYPE 306): 1.o pedido por el cliente, 2.o por deuda, 3.o por exceder el limite de credito; "
                  "0 sin bloqueo, 1 una via (salientes), 2 dos vias. 000 = normal. El 3.er digito solo aparece en 27 lineas (marca heredada). "
                  "La preactivacion NO va aqui: es STATUS 9 con 000. Solo describe la linea si STATUS = 2. Para comparar con PRE: substr(ACT_STATUS, 1, 2). "
                  "En palabras: silver.f_bloqueo(origen, ACT_STATUS)",
    "STA_DATETIME": "Alta del periodo. OJO: se hereda en la migracion pre<->pos",
    "END_DATETIME": "Baja o cierre del periodo (NULL si sigue abierto)",
    "MIG_DATETIME": "Fecha de migracion hacia esta linea; BI_REPORT usa NVL(MIG_DATETIME, STA_DATETIME) como inicio",
    "CONTRACT_ID": "Contrato: el cliente de una linea pospago sale de CM_POS.CONTRACT.CUST_ID",
  },
}
tablas = {
  "bronze.cm_pre__sub_mb": "Lineas prepago (CM_PRE.SUB_MB). Cliente: CUST_ID -> CM_POS.CUSTOMER",
  "bronze.cm_pos__sub_mb": "Lineas pospago (CM_POS.SUB_MB). Cliente: CONTRACT_ID -> CM_POS.CONTRACT -> CM_POS.CUSTOMER",
}
 
def texto(t):
    return t.replace("'", "\\'")
 
for tabla, cols in comentarios.items():
    existentes = {c.upper() for c in spark.table(tabla).columns}
    for col, txt in cols.items():
        if col in existentes:
            spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN {col} COMMENT '{texto(txt)}'")
            print(f"ok    {tabla}.{col}")
        else:
            print(f"falta {tabla}.{col} (no existe en esta tabla, se omite)")
    spark.sql(f"COMMENT ON TABLE {tabla} IS '{texto(tablas[tabla])}'")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE OR REPLACE TABLE gold.diccionario_columnas AS SELECT ... FROM information_schema.columns WHERE table_schema = 'bronze'
# MAGIC CREATE OR REPLACE TABLE gold.catalogo_valores
# MAGIC COMMENT 'Valores de conjuntos cerrados de bronze: significado, nombre oficial (AP_DOMAIN), evidencia y confianza. Se regenera con este CTAS'
# MAGIC AS
# MAGIC WITH oficial AS (   -- catalogo oficial de ACT_STATUS
# MAGIC   SELECT trim(TYPE) AS tipo, trim(CODE) AS codigo, trim(NAME) AS nombre_oficial
# MAGIC   FROM bronze.cm_pos__ap_domain
# MAGIC   WHERE trim(TYPE) IN ('306', '02'))
# MAGIC SELECT tabla, columna, valor, significado, CAST(NULL AS STRING) AS nombre_oficial, evidencia, confianza FROM VALUES
# MAGIC   ('cm_pre__sub_mb', 'STATUS', '2', 'Activa. Si ACT_STATUS contiene 3 es un kit preactivado sin primer uso: los reportes OSIPTEL de BI_REPORT lo excluyen con ACT_STATUS NOT LIKE %3%',
# MAGIC      'BI_REPORT: pck_lsc_report, pkg_execute_lsc_2019, sp_lsc_subs (ACTIVE); pck_stock_isdn_mobile (Kit)', 'codigo + BCCS'),
# MAGIC   ('cm_pre__sub_mb', 'STATUS', '3', 'Cancelada', 'BI_REPORT: CANCELLED en los 3 paquetes LSC; BCCS Cancelled', 'codigo + BCCS'),
# MAGIC   ('cm_pre__sub_mb', 'STATUS', '1', 'Registrada sin conectar', 'BI_REPORT: NOT CONNECT en los 3 paquetes LSC', 'codigo'),
# MAGIC   ('cm_pre__sub_mb', 'STATUS', '0', 'Eliminada (BCCS Delete). Con ACT_STATUS 03 y sin fechas: kit borrado antes de usarse. Con baja: probable cierre al pasar a pospago',
# MAGIC      'sp_lsc_subs la cuenta como CANCELLED; BCCS: 3 fichas Delete, todas sin fecha de activacion (1 cruzada con SQL: STATUS 0, ACT_STATUS 03, sin fechas); en CM_PRE la accion 167 = Internal cancel line - Transfer to postpaid', 'parcial: medir % de 167 con la huella de acciones'),
# MAGIC   ('cm_pre__sub_mb', 'STATUS', '4', 'Cancelada en un proceso masivo: baja 13/06/2019 00:30:53; los casos revisados tienen ACT_STATUS 03 (kits preactivados)',
# MAGIC      'BCCS 5 casos: Cancelled, misma fecha de baja', 'alta para la fecha; causa por confirmar'),
# MAGIC   ('cm_pre__sub_mb', 'STATUS', '9', 'Sin significado documentado en prepago (en pospago es preactivada); revisar caso a caso',
# MAGIC      'ninguna', 'desconocido'),
# MAGIC   ('cm_pos__sub_mb', 'STATUS', '2', 'Activa', 'BI_REPORT: ACTIVE en los 3 paquetes LSC', 'codigo + BCCS'),
# MAGIC   ('cm_pos__sub_mb', 'STATUS', '3', 'Cancelada', 'BI_REPORT: CANCELLED; BCCS Cancelled', 'codigo + BCCS'),
# MAGIC   ('cm_pos__sub_mb', 'STATUS', '1', 'Registrada sin conectar', 'BI_REPORT: NOT CONNECT; logs: 84% con accion 00 y ninguna con 666', 'codigo + logs'),
# MAGIC   ('cm_pos__sub_mb', 'STATUS', '9', 'Preactivada: suscriptor recibido (accion 666) que aun no tiene la conexion (accion 00)',
# MAGIC      'sp_lsc_subs y pkg_execute_lsc_2019: STATUS 9 + ACT_STATUS 000 = PRE-ACTIVATED; BCCS 2 casos: Inactive; logs: 33.7% con 666 y 0% con 00 (las activas con 666 siempre tienen 00)', 'codigo + BCCS + logs'),
# MAGIC   ('cm_pos__sub_mb', 'STATUS', '0', 'Inactiva: cerrada al pasar a prepago',
# MAGIC      'BCCS 3 casos: Inactive + Internal cancel line - Transfer to prepaid; logs: 34% con accion 167 (Internal cancel line - Transfer to prepaid) y 16.7% con 668; sp_lsc_subs la cuenta como CANCELLED', 'codigo + BCCS + logs'),
# MAGIC   ('cm_pos__sub_mb', 'STATUS', '4', 'Cerrada por cambio de titular; la mayoria con la misma baja 12/06/2021 10:37:50',
# MAGIC      'BCCS 7 casos: Changed contract owner (contrato Cancelled o Terminated)', 'alta')
# MAGIC AS v(tabla, columna, valor, significado, evidencia, confianza)
# MAGIC UNION ALL
# MAGIC SELECT 'cm_pre__sub_mb', 'ACT_STATUS', v.ACT_STATUS, silver.f_bloqueo('PRE', v.ACT_STATUS), o.nombre_oficial,
# MAGIC        CASE WHEN o.codigo IS NOT NULL THEN 'CM_POS.AP_DOMAIN TYPE 02 (lo llama in debt; el HLR de prepago, CM_PRE TYPE 100, lo llama VMS = sistema)'
# MAGIC             WHEN v.ACT_STATUS RLIKE '^[0-2]3$' THEN 'CM_PRE.AP_DOMAIN TYPE 100 (entrantes bloqueadas); BI_REPORT: sp_lsc_subs, pkg_execute_lsc_2019 (PRE-ACTIVATED), pck_stock_isdn_mobile (Kit)'
# MAGIC             WHEN v.ACT_STATUS RLIKE '^[0-2]4$' THEN 'CM_PRE.AP_DOMAIN TYPE 100 (chan het tien = sin saldo)'
# MAGIC             ELSE 'ninguna' END,
# MAGIC        CASE WHEN o.codigo IS NOT NULL THEN 'catalogo oficial'
# MAGIC             WHEN v.ACT_STATUS RLIKE '^[0-2][34]$' THEN 'catalogo HLR + codigo' ELSE 'fuera del catalogo' END
# MAGIC FROM (SELECT DISTINCT ACT_STATUS FROM bronze.cm_pre__sub_mb) v
# MAGIC LEFT JOIN oficial o ON o.tipo = '02' AND o.codigo = v.ACT_STATUS
# MAGIC UNION ALL
# MAGIC SELECT 'cm_pos__sub_mb', 'ACT_STATUS', v.ACT_STATUS, silver.f_bloqueo('POS', v.ACT_STATUS), o.nombre_oficial,
# MAGIC        CASE WHEN o.codigo IS NOT NULL THEN 'CM_POS.AP_DOMAIN TYPE 306' ELSE 'ninguna' END,
# MAGIC        CASE WHEN o.codigo IS NOT NULL THEN 'catalogo oficial' ELSE 'fuera del catalogo' END
# MAGIC FROM (SELECT DISTINCT ACT_STATUS FROM bronze.cm_pos__sub_mb) v
# MAGIC LEFT JOIN oficial o ON o.tipo = '306' AND o.codigo = v.ACT_STATUS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW silver.v_catalogo_acciones
# MAGIC COMMENT 'ACTION_CODE de ACTION_AUDIT con su nombre, por origen: CM_POS y CM_PRE .AP_DOMAIN TYPE 20'
# MAGIC AS
# MAGIC SELECT 'POS' AS origen, trim(CODE) AS action_code, trim(NAME) AS nombre, trim(NAME_ES) AS nombre_es
# MAGIC FROM bronze.cm_pos__ap_domain WHERE trim(TYPE) = '20'
# MAGIC UNION ALL
# MAGIC SELECT 'PRE', trim(CODE), trim(NAME), trim(NAME_ES)
# MAGIC FROM bronze.cm_pre__ap_domain WHERE trim(TYPE) = '20';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.diccionario_columnas
# MAGIC COMMENT 'Diccionario de columnas de bronze (information_schema + comentarios). Se regenera con este CTAS'
# MAGIC AS
# MAGIC SELECT table_catalog, table_schema, table_name, column_name, ordinal_position,
# MAGIC        data_type, is_nullable, comment
# MAGIC FROM workspace.information_schema.columns
# MAGIC WHERE table_schema = 'bronze';
# MAGIC  
# MAGIC -- Terminado cuando: SUB_MB tiene sus comentarios en el diccionario
# MAGIC SELECT table_name, column_name, comment
# MAGIC FROM gold.diccionario_columnas
# MAGIC WHERE table_name LIKE '%sub_mb' AND comment IS NOT NULL
# MAGIC ORDER BY 1, 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Un valor que aparezca como «(sin catalogar)» es nuevo en los datos: agrégalo a la celda 4.
# MAGIC WITH n AS (
# MAGIC   SELECT 'cm_pre__sub_mb' AS tabla, 'STATUS' AS columna, STATUS AS valor, count(*) AS lineas FROM bronze.cm_pre__sub_mb GROUP BY ALL
# MAGIC   UNION ALL SELECT 'cm_pos__sub_mb', 'STATUS', STATUS, count(*) FROM bronze.cm_pos__sub_mb GROUP BY ALL
# MAGIC   UNION ALL SELECT 'cm_pre__sub_mb', 'ACT_STATUS', ACT_STATUS, count(*) FROM bronze.cm_pre__sub_mb GROUP BY ALL
# MAGIC   UNION ALL SELECT 'cm_pos__sub_mb', 'ACT_STATUS', ACT_STATUS, count(*) FROM bronze.cm_pos__sub_mb GROUP BY ALL)
# MAGIC SELECT coalesce(c.tabla, n.tabla) AS tabla, coalesce(c.columna, n.columna) AS columna, coalesce(c.valor, n.valor) AS valor,
# MAGIC        coalesce(c.significado, '(sin catalogar)') AS significado, c.nombre_oficial, c.confianza, n.lineas, c.evidencia
# MAGIC FROM gold.catalogo_valores c
# MAGIC FULL JOIN n ON n.tabla = c.tabla AND n.columna = c.columna AND n.valor <=> c.valor
# MAGIC ORDER BY tabla, columna, lineas DESC NULLS LAST;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT origen, silver.f_estado_linea(origen, STATUS, ACT_STATUS) AS estado_final, count(*) AS lineas,
# MAGIC        round(100 * count(*) / sum(count(*)) OVER (PARTITION BY origen), 2) AS pct
# MAGIC FROM (SELECT 'PRE' AS origen, STATUS, ACT_STATUS FROM bronze.cm_pre__sub_mb
# MAGIC       UNION ALL
# MAGIC       SELECT 'POS', STATUS, ACT_STATUS FROM bronze.cm_pos__sub_mb)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY origen, lineas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preguntas tipo examen
# MAGIC 1. ¿Qué pasa con los datos al hacer `DROP TABLE` en una tabla administrada? ¿Y en una externa?
# MAGIC 2. Una vista temporal creada en un notebook, ¿se puede consultar desde un job distinto?
# MAGIC 3. ¿Qué comando reemplaza todos los datos de una tabla sin cambiar su esquema ni su historial, y falla si el esquema no calza?
# MAGIC 4. ¿Cuál es la jerarquía de objetos de Unity Catalog?
# MAGIC
# MAGIC <details><summary>Respuestas</summary>
# MAGIC
# MAGIC 1. Administrada: se borran metadatos y datos. Externa: solo metadatos; los archivos quedan.
# MAGIC 2. No: vive solo en la sesión que la creó.
# MAGIC 3. `INSERT OVERWRITE`.
# MAGIC 4. Metastore → catálogo → esquema → tablas, vistas, volúmenes, funciones y modelos.
# MAGIC </details>
# MAGIC