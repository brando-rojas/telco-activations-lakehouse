# Databricks notebook source
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
# MAGIC - Pospago: línea → cliente pasa por el contrato (`CM_POS.SUB_MB → CM_POS.CONTRACT → CM_POS.CUSTOMER`). Prepago: `CM_PRE.SUB_MB.CUST_ID → CM_POS.CUSTOMER`.
# MAGIC - STATUS 2 activa, 3 cancelada; ACT_STATUS con 1 = bloqueo 1 vía, con 2 = bloqueo 2 vías.
# MAGIC - El STA_DATETIME **se hereda** en la migración pre↔pos.
# MAGIC - El hito de alta en ACTION_AUDIT es ACTION_CODE '00', a 15 minutos como máximo del STA_DATETIME.

# COMMAND ----------

# MAGIC %run ./00_config

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
pos = None

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
# MAGIC - líneas canceladas (STATUS 3) **sin** fecha de baja, y activas (STATUS 2) **con** fecha de baja.
# MAGIC
# MAGIC **Terminado cuando:** tienes una tabla de 2 filas (PRE, POS) con esos 5 conteos. Cada número distinto de cero es un hallazgo para el diccionario.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.4 · Transiciones con ventanas (bien hecho)
# MAGIC Ahora define un **inicio efectivo** de cada periodo que no dependa del STA heredado. Opciones (elige y justifica en `docs/decisiones.md`):
# MAGIC - para un POS con gemelo PRE: la `baja_ts` del PRE (el prepago se cierra cuando nace el pospago);
# MAGIC - o `mig_ts` si viene lleno y es coherente;
# MAGIC - o la fecha del trámite en MIGRATION_REQUESTS / REQUEST_TRANSFER_POSTPAID (E4.5).
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.5 · Contrastar con los trámites de migración
# MAGIC Los trámites dicen **cuándo se pidió** la migración. **Tareas:**
# MAGIC 1. Mira las columnas de `bronze.cm_pos__migration_requests` y `bronze.cm_pre__request_transfer_postpaid`: ¿cuál es el número, cuál la fecha, cuál el estado del trámite?
# MAGIC 2. Une cada transición `PRE→POS` con su trámite por `isdn9` y calcula la diferencia en días entre el trámite y tu `inicio_ts`.
# MAGIC 3. Distribución: mismo día, 1–3 días, más de 3, **sin trámite**.
# MAGIC 4. Al revés: trámites aprobados que **no** tienen transición en SUB_MB.
# MAGIC
# MAGIC **Terminado cuando:** puedes decir qué campo de qué tabla da la fecha real de la migración, con un porcentaje que lo respalde.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze.cm_pos__migration_requests

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze.cm_pre__request_transfer_postpaid

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.6 · Validar con ACTION_AUDIT (solo en la ventana de logs)
# MAGIC Los logs cubren solo los últimos `DIAS_LOGS` días. Dentro de esa ventana:
# MAGIC 1. Toma las líneas (PRE y POS) con `alta_ts` en la ventana.
# MAGIC 2. Busca su hito ACTION_CODE '00' en ACTION_AUDIT (por `PK_ID` = `sub_id`; revisa en `PK_TYPE` cuál es el tipo de una línea).
# MAGIC 3. Calcula la diferencia en minutos. Regla: ≤ 15 min es normal.
# MAGIC 4. Compara: líneas **con** gemelo (STA heredado) vs **sin** gemelo. ¿En cuál falla más la regla de los 15 minutos?
# MAGIC
# MAGIC **Terminado cuando:** tienes el % que cumple la regla en cada grupo. Si el STA se hereda, las migradas deberían fallar casi siempre: su alta real es otra.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.7 · Titularidad
# MAGIC **Tareas:**
# MAGIC 1. `DESCRIBE bronze.cm_pos__change_owner_log`: ¿qué columnas son el titular anterior, el nuevo, la línea y la fecha?
# MAGIC 2. Para cada línea, el **último** cambio (ventana `ROW_NUMBER() ... ORDER BY fecha DESC`).
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.8 · Funciones de orden superior: la historia de un número en una fila
# MAGIC **Tareas:**
# MAGIC 1. Por `isdn9`, junta sus periodos en un arreglo ordenado: `array_sort(collect_list(struct(inicio_ts, tipo, sub_id, status)))`.
# MAGIC 2. Con `transform`, arma un texto legible tipo `PRE(2019-03) → POS(2023-07) → PRE(2025-01)`.
# MAGIC 3. Con `filter` y `size`, cuenta cuántos periodos POS tiene cada número; con `exists`, marca los que alguna vez fueron pospago.
# MAGIC 4. Muestra los 20 números con más periodos.
# MAGIC 5. Haz el camino inverso con `explode` y comprueba que vuelves al mismo número de filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.9 · Pivot: STATUS × ACT_STATUS
# MAGIC **Tarea:** una tabla por `tipo` con STATUS en filas, ACT_STATUS en columnas (los 6 más frecuentes) y conteos. Decodifica con `silver.v_dominio` (cap. 3).
# MAGIC **Pregunta:** ¿hay líneas STATUS 3 (canceladas) con bloqueo activo? ¿Qué significaría?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: SELECT * FROM (SELECT tipo, status, act_status FROM silver.lineas) PIVOT (count(*) FOR act_status IN (...))

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4.10 · UDF vs función nativa
# MAGIC **Tarea:** escribe una UDF de Python que normalice el número (lo mismo que `isdn9`) y compara el tiempo de las dos sobre `bronze.cm_pre__sub_mb`.
# MAGIC Mira el Query Profile: ¿qué cambia cuando hay una UDF de Python?

# COMMAND ----------

from pyspark.sql.types import StringType

@F.udf(StringType())
def isdn9_udf(x):
    if x is None:
        return None
    d = "".join(ch for ch in x if ch.isdigit())
    return d[-9:] if d else x

# TODO: mide con time.time() un count() usando isdn9_udf y otro usando F.expr(isdn9(COL['isdn']))

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

