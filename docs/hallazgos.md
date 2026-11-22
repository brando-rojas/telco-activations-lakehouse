# Hallazgos · línea de tiempo de cada número (cap. 8)

> Resultados del notebook `08_cierre_linea_de_tiempo` sobre el export seudonimizado de BCCS (Oracle → Parquet → Databricks Free Edition).
> **Solo agregados:** ningún número de teléfono, cliente ni fila individual.
> Corte del export: eventos hasta el 05–06/11/2026.

## Lo más importante

1. **Contando solo los estados oficiales, las portabilidades de entrada salen a la mitad.** Hay 1,03 M solicitudes de entrada en estado `BYPASS` / `M_BYPASS`. No figuran en el catálogo `MNP.STATE` ni tienen fecha de ejecución, pero en el 82–88 % nace una línea Bitel con ese número en los 10 días siguientes. Con ellas, las entradas pasan de 883 mil a 1,91 M: el 53,8 % de las entradas no tiene estado oficial de "ejecutada".
2. **Dos tablas de CM_PRE no son lo que su nombre dice:**
   - `CM_PRE.TERMINATION_SUB` no registra solicitudes de baja, sino la baja ya hecha: el 98,8 % de sus filas cae justo después del fin de la línea, a 3 días o menos.
   - `CM_PRE.SUB_SIM_TRANSFER` no registra cambios de SIM: el 97,6 % es la SIM con que nació la línea. Los cambios reales son unos 78 mil.
3. **Desde febrero de 2026 se disparan las migraciones pospago → prepago sin trámite.** Pasan de unas 2,4 mil al mes en 2025 a unas 23 mil al mes entre febrero y octubre de 2026, que es el 41,7 % de las POS→PRE de ese periodo. Algo cambió en el proceso (un canal o una migración automática) y no deja registro en `TRANSFER_REQUEST`.
4. **Los números activos en prepago y pospago a la vez son migraciones mal cerradas.**
   - Son 1.245 números.
   - El 82,9 % es una migración PRE→POS que no cerró el prepago, y el 5,3 % una POS→PRE que no cerró el pospago.
   - Solo el 11,8 % no tiene rastro de migración.
   - Con la definición del reporte a ITOps (sin bloqueos) hoy son 1.054, no 544.
5. **544 números se portaron a otro operador y su línea Bitel nunca se dio de baja** (0,055 % de las salidas).
6. **La mitad de las líneas que cambian de titular se da de baja en 90 días (53,2 %).** Está por validar, porque "baja" también incluye migraciones (ver pendientes).
7. **La regla del gemelo del cap. 4 falla en las líneas que luego se ceden.** Afecta a 68 mil periodos, el 0,09 % del historial (sección 2).
8. **La regla R1 no mide inconsistencias, sino cuánto cubren los logs.** El 76 % de las bajas pospago no tiene ninguna solicitud, portabilidad, migración ni cambio que la explique, porque cada fuente cubre un periodo distinto: las portabilidades solo desde 03/2026 y las solicitudes de baja desde 2020.

## 1. Qué se construyó

| Tabla | Qué es |
|---|---|
| `silver.tenencia` | Quién tuvo cada número y desde/hasta cuándo: 75,46 M periodos (uno por línea). |
| `gold.linea_tiempo` | Un evento por fila, con fecha real, línea (`sub_id`) y fuente: ~230 M eventos. |
| `gold.inconsistencias` | Casos en que las fuentes se contradicen (reglas R1 a R5). |
| `gold.diccionario_columnas` | Columnas de bronze; 16 columnas clave tienen su significado según los datos, su confiabilidad y la fuente recomendada. |

**Eventos por fuente (`gold.linea_tiempo`)**

| Fuente | Evento | Eventos | Números | Desde | Hasta |
|---|---|---:|---:|---|---|
| silver.tenencia | ALTA | 75,46 M | 21,78 M | 07/06/2011 | 28/11/2026* |
| silver.lineas | BAJA | 68,03 M | 20,22 M | 16/02/2013 | 05/11/2026 |
| silver.transiciones | RECICLAJE | 42,51 M | 13,43 M | 21/05/2013 | 05/11/2026 |
| silver.transiciones | MIGRACION | 8,03 M | 5,53 M | 26/02/2014 | 28/11/2026* |
| cm_pre.termination_sub | SOLICITUD_BAJA (en realidad, baja registrada) | 24,38 M | 11,23 M | 06/03/2015 | 04/11/2026 |
| cm_pre.sub_sim_transfer | CAMBIO_SIM (97,6 % es la SIM del alta) | 3,32 M | 2,12 M | 23/02/2012 | 28/11/2026* |
| cm_pos.req_change_sim | CAMBIO_SIM | 3,15 M | 2,12 M | 29/03/2023 | 05/11/2026 |
| cm_pos.terminate_req | SOLICITUD_BAJA | 1,35 M | 1,26 M | 01/01/2020 | 05/11/2026 |
| cm_pos.change_owner_log | CAMBIO_TITULAR | 502 mil | 359 mil | 10/11/2021 | 05/11/2026 |
| cm_pos.sub_change_isdn | CAMBIO_NUMERO | 14 mil | 11 mil | 17/08/2021 | 04/11/2026 |
| mnp.porting_request | PORTABILIDAD_IN | 1,91 M | 984 mil | 15/03/2026 | 06/11/2026 |
| mnp.porting_request | PORTABILIDAD_OUT | 997 mil | 974 mil | 16/03/2026 | 06/11/2026 |
| mnp.porting_request | RETORNO_IN (el número vuelve a Bitel) | 393 mil | 387 mil | 08/04/2026 | 06/11/2026 |
| mnp.porting_request | RETORNO_OUT (Bitel lo devuelve) | 383 mil | 383 mil | 16/03/2026 | 05/11/2026 |

\* Hay fechas posteriores al corte del export (hasta el 28/11/2026) en tablas de CM_PRE. Son pocas, pero cualquier análisis "a hoy" debe cortar en la fecha del export, como hace E8.4-5.

**Cada fuente cubre un periodo distinto.** Las portabilidades solo existen desde marzo de 2026, los cambios de titular desde noviembre de 2021 y las solicitudes de baja pospago desde 2020. Cualquier regla que cruce fuentes solo es justa dentro de la ventana común.

## 2. Calidad del historial (`silver.tenencia`)

**Inicio del periodo**
- STA_DATETIME: 89,9 %.
- MIG_DATETIME: 9,1 %.
- Cambio de titular: 0,6 %.
- Baja del gemelo: 0,4 %.

**Fin del periodo**
- Baja: 84,2 %.
- Vigente: 15,1 %.
- Cedida en un cambio de titular: 0,6 %.
- Inicio de la siguiente línea: 0,1 %.

**Problemas encontrados**

| Problema | Periodos | % | Lectura |
|---|---:|---:|---|
| `hasta` anterior a `desde` | 71.254 | 0,09 % | El 95 % viene de una sola combinación: inicio por **baja del gemelo** y fin por **cesión**. En el 99,9 % (POS) y el 99,8 % (PRE) de esos periodos, la línea se cedió *antes* de la fecha que la regla del gemelo le pone como inicio. Esa regla del cap. 4 no debe aplicarse a líneas que aparecen como cedidas en `CHANGE_OWNER_LOG`. |
| Se solapa con la línea anterior del número | 1,83 M | 2,4 % | El 86,6 % de los solapes son periodos que empiezan por MIG_DATETIME: el sistema abre la línea nueva antes de registrar la baja de la anterior. No medimos cuánto dura ese solape (ver pendientes). |

## 3. Cruce por número + fecha (tablas sin SUB_ID)

`MNP.PORTING_REQUEST`, `CM_PRE.TERMINATION_SUB` y `CM_PRE.SUB_SIM_TRANSFER` no traen SUB_ID. Para cada evento se buscó en `silver.tenencia` qué línea tenía ese número en esa fecha, con una tolerancia de ±3 días.

| Fuente · evento | Eventos | Dentro del periodo | Cerca (≤ 3 días) | Sin línea | Lectura |
|---|---:|---:|---:|---:|---|
| Portabilidad de salida | 997 mil | 99,99 % | 0,01 % | 0,00 % | El cruce funciona: la línea que se va es la que tenía el número. |
| Portabilidad de entrada | 1,91 M | 1,3 % | 89,8 % | 8,9 % | Es normal que caiga "cerca": la ejecución ocurre minutos antes de que nazca la línea. El 8,9 % sin línea son, en su mayoría, BYPASS que no se concretaron (se esperaban unos 164 mil y salen 170 mil). |
| SUB_SIM_TRANSFER · CAMBIO_SIM | 3,32 M | 88,7 % | 10,2 % | 1,1 % | El 97,6 % está a menos de 1 día del alta de la línea: es la SIM inicial. |
| TERMINATION_SUB · SOLICITUD_BAJA | 24,38 M | 0,7 % | 98,8 % | 0,5 % | Casi todo cae justo *después* del periodo: es el registro de la baja, no una solicitud. |
| Retorno: el número vuelve a Bitel | 393 mil | 0,2 % | — | 99,8 % | Esperado: el número entra al stock sin línea. |
| Retorno: Bitel devuelve el número | 383 mil | 0,3 % | 0,0 % | 99,7 % | Esperado: se devuelve días después de la baja. |

**La prueba con algo ya sabido (E8.1-c):** en el cap. 4 vimos que en portabilidad el STA de la línea es la fecha de ejecución. Desde el lado de MNP sale lo mismo: el 91,1 % de las entradas encuentra su línea, y en el 89,2 % esa línea nace a 24 horas o menos de la ejecución.

## 4. Portabilidad (E8.0)

- **Bitel es el operador 24.** El TYPE ya dice el sentido:
  - **01:** entrada. Bitel recibe en el 100 %.
  - **05:** salida. Bitel cede en el 100 %, salvo 419 consultas previas.
  - **02:** retorno del número a Bitel.
  - **06:** Bitel devuelve el número a su operador original.
- **Estados con fecha de ejecución:** `00A02` ("Process finished successfully") y `01A06` ("Porting successfully completed").
  - `02D04` tiene fecha, pero su estado es de rechazo, así que queda fuera.
  - `BYPASS` y `M_BYPASS` no tienen ni estado en el catálogo ni fecha.
- **`PORT_NUMBER` calza con un número de SUB_MB** en el 93,2 % de TYPE 01 y en el 99,9–100 % del resto. `f_isdn9` lo limpia bien.

**Volumen en la ventana del export (15/03 al 06/11/2026)**

| | Solicitudes | Ejecutadas | Lo demás |
|---|---:|---:|---|
| Entrada (01) | 2,80 M | 883 mil oficiales + 1,03 M BYPASS | 617 mil consultas previas, 262 mil canceladas |
| Salida (05) | 1,32 M | 997 mil | 253 mil consultas previas de otros operadores |
| Retorno a Bitel (02) | 884 mil | 393 mil | 253 mil retornos rechazados |
| Retorno desde Bitel (06) | 391 mil | 383 mil | 8 mil en `02D04` |

**¿Los BYPASS son portabilidades reales? (E8.0-4)** Se midió en qué % de las solicitudes nace una línea Bitel con ese número entre 1 día antes y 10 días después de la solicitud:

| Estado | Solicitudes | Nace una línea | Con `PORT_IN_ACTIVE_DATE` |
|---|---:|---:|---:|
| 00A02 | 882.007 | 99,8 % | 61,1 % |
| BYPASS | 394.658 | 87,7 % | 0 % |
| M_BYPASS | 635.336 | 81,8 % | 0 % |

**Conclusión:** son entradas reales. En `gold.linea_tiempo` entran con la fecha de la solicitud y lo dicen en el detalle (`fecha=solicitud (BYPASS sin ejecucion)`). Contando las BYPASS, en la ventana del export Bitel recibió por portabilidad 1,9 veces lo que cedió.

## 5. Preguntas de E8.4

### 1) Migraciones PRE↔POS por mes, con y sin trámite

<details><summary>Consulta</summary>

```sql
WITH mig AS (SELECT * FROM silver.transiciones WHERE tipo_ant <> tipo AND mismo_cliente),
con_tramite AS (
  SELECT DISTINCT m.isdn9, m.sub_id
  FROM mig m
  JOIN silver.tramites_migracion r
    ON r.isdn9 = m.isdn9 AND r.sentido = m.transicion
   AND abs(datediff(m.inicio_ts, r.tramite_ts)) <= 3)
SELECT date_trunc('month', m.inicio_ts) AS mes, m.transicion,
       count(*) AS migraciones, count(c.sub_id) AS con_tramite, count(*) - count(c.sub_id) AS sin_tramite
FROM mig m
LEFT JOIN con_tramite c ON c.isdn9 = m.isdn9 AND c.sub_id = m.sub_id
GROUP BY 1, 2 ORDER BY 1, 2;
```
</details>

**Resultado por año**

| Año | PRE→POS | Sin trámite | POS→PRE | Sin trámite |
|---|---:|---:|---:|---:|
| 2014 | 657 | 100 % | 576 | 100 % |
| 2015 | 26.869 | 2,5 % | 3.109 | 27,5 % |
| 2016 | 120.822 | 1,7 % | 27.181 | 7,8 % |
| 2017 | 213.560 | 4,4 % | 86.650 | 7,2 % |
| 2018 | 379.770 | 6,6 % | 163.036 | 10,3 % |
| 2019 | 321.817 | 10,8 % | 154.433 | 7,4 % |
| 2020 | 231.829 | 13,6 % | 136.683 | 4,6 % |
| 2021 | 413.391 | 11,0 % | 262.746 | 3,8 % |
| 2022 | 572.122 | 8,1 % | 358.735 | 5,8 % |
| 2023 | 555.121 | 11,5 % | 300.209 | 6,9 % |
| 2024 | 624.738 | 12,8 % | 252.464 | 8,7 % |
| 2025 | 1.042.959 | 10,1 % | 298.488 | 9,5 % |
| 2026 (hasta nov.) | 935.682 | 11,4 % | 545.080 | **40,7 %** |

- **En total:** 8,03 M migraciones, el 88,5 % con trámite. Antes de 2015 no hay logs de trámite.
- **PRE→POS crece fuerte:** unas 52 mil al mes en 2024, 87 mil en 2025 y 92 mil en 2026.
- **POS→PRE cambia en febrero de 2026:** las migraciones sin trámite pasan de unas 2,4 mil al mes a unas 23 mil al mes (feb–oct). Las con trámite también suben, de 22,5 mil a 32,4 mil al mes.
- **El confinamiento de 2020 se ve en los datos:** las PRE→POS caen de 23 mil en febrero de 2020 a 2 en abril y 16 en mayo.

### 2) % de altas pospago con STA_DATETIME heredado
**28,22 %.** Es la misma cifra de E4.3: el resultado es consistente entre capítulos.

### 3) Tiempo de la solicitud a la ejecución de una portabilidad
Solo estados `00A02` y `01A06` (las BYPASS no tienen fecha de ejecución).

| TYPE | Portabilidades | Promedio | Mediana |
|---|---:|---:|---:|
| 01 entrada | 883.286 | 0,35 días | 8 h |
| 05 salida | 996.657 | 0,35 días | 8 h |
| 02 retorno a Bitel | 383.417 | 1,02 días | 25 h |
| 06 retorno desde Bitel | 382.914 | 0,70 días | 19 h |

### 4) Números reciclados (pasan a otro cliente) y tiempo libre
No incluye los cambios de titular.

- 13,40 M números reciclados.
- 42,38 M reciclajes: unas 3,2 veces por número.
- Tiempo libre: mediana de 170 días y promedio de 356. El promedio sube por los números que pasan años sin asignarse.

### 5) Líneas que cambiaron de titular y se dieron de baja en 90 días
Solo cambios con 90 días de observación antes del corte.

- 472.170 líneas recibidas por cambio de titular.
- 251.055 (**53,2 %**) se dieron de baja en los 90 días siguientes.
- Desde el historial, de las 487 mil líneas nacidas por cambio de titular, el 68 % ya tiene baja y el 20,7 % volvió a cederse (cadenas de titulares).

## 6. Reglas de inconsistencia (`gold.inconsistencias`)

| Regla | Casos | Números | Lectura |
|---|---:|---:|---|
| R1 · baja pospago sin solicitud ni portabilidad en ±3 días | 12,63 M | 9,72 M | 76 % de las 16,6 M bajas pospago. El 98,2 % no tiene ninguna explicación y el 1,8 % (230,7 mil) es una baja automática (`AUTO_TERMINATE_SUB`). Mide la cobertura de los logs, no errores. |
| R2 · portabilidad de salida sin baja después | 544 | 542 | La línea sigue viva en Bitel después de portarse fuera. Parte puede ser de los últimos días antes del corte. |
| R3 · cambio de titular sobre una línea ya dada de baja | 12 | 11 | `CHANGE_OWNER_LOG` es consistente con SUB_MB. |
| R4 · evento antes de la primera alta del número | 2,13 M | 1,90 M | Falta desglosarla por fuente. Hipótesis: SUB_MB no conserva las líneas más antiguas y otros logs sí. |
| R5 · número activo en prepago y pospago a la vez | 1.249 | 1.245 | Ver causas abajo. |

**R5 por causa**

| Causa | Números | % |
|---|---:|---:|
| Migración PRE→POS que no cerró el prepago | 1.032 | 82,9 % |
| Sin rastro de migración | 147 | 11,8 % |
| Migración POS→PRE que no cerró el pospago | 66 | 5,3 % |

Con la misma definición del reporte enviado a ITOps (prepago `00` y pospago `000`, sin bloqueos) salen **1.054** números, no 544. No conocemos la fecha de corte de aquel reporte.

## 7. Qué confirma de los capítulos anteriores

- **STA heredado en el 28,22 % de las altas pospago:** igual que en E4.3.
- **En portabilidad, el STA es la ejecución (E4.6):** confirmado desde MNP, con el 89,2 % de las líneas naciendo a ≤ 24 h.
- **`CHANGE_OWNER_LOG` es confiable:** solo 12 cesiones sobre líneas ya dadas de baja.
- **El cruce por número + fecha reemplaza bien al SUB_ID:** 99,99 % en las salidas.

## 8. Pendientes

1. **R1 en la ventana común**, donde todas las fuentes existen:
   ```sql
   SELECT (SELECT count(*) FROM gold.inconsistencias
           WHERE regla LIKE 'R1%' AND fecha_ts >= '2026-03-16') AS r1,
          (SELECT count(*) FROM gold.linea_tiempo
           WHERE tipo_evento = 'BAJA' AND tipo_linea = 'POS' AND fecha_ts >= '2026-03-16') AS bajas_pos;
   ```
2. **R4 por fuente:**
   ```sql
   SELECT split(detalle, '; ')[0] AS tipo_evento, split(detalle, '; ')[1] AS fuente, count(*) AS casos
   FROM gold.inconsistencias WHERE regla LIKE 'R4%' GROUP BY 1, 2 ORDER BY casos DESC;
   ```
3. **¿Cuántas de las bajas tras un cambio de titular son migraciones, portabilidades o cambios de número?**
   ```sql
   WITH ct AS (SELECT isdn9, sub_id, fecha_ts FROM gold.linea_tiempo WHERE tipo_evento = 'CAMBIO_TITULAR'),
   b AS (SELECT l.isdn9, l.sub_id, l.fecha_ts FROM gold.linea_tiempo l
         JOIN ct ON l.sub_id = ct.sub_id AND l.isdn9 = ct.isdn9
         WHERE l.tipo_evento = 'BAJA' AND l.fecha_ts BETWEEN ct.fecha_ts AND ct.fecha_ts + INTERVAL 90 DAYS)
   SELECT count(DISTINCT b.sub_id) AS bajas_90d,
          count(DISTINCT CASE WHEN m.isdn9 IS NOT NULL THEN b.sub_id END) AS explicadas
   FROM b LEFT JOIN gold.linea_tiempo m
     ON m.isdn9 = b.isdn9 AND m.tipo_evento IN ('MIGRACION', 'PORTABILIDAD_OUT', 'CAMBIO_NUMERO')
    AND m.fecha_ts BETWEEN b.fecha_ts - INTERVAL 3 DAYS AND b.fecha_ts + INTERVAL 3 DAYS;
   ```
4. **Cuánto duran los solapes de las migraciones:**
   ```sql
   WITH s AS (SELECT *, LAG(hasta) OVER (PARTITION BY isdn9 ORDER BY desde, tipo, sub_id) AS hasta_anterior
              FROM silver.tenencia)
   SELECT fuente_desde, count(*) AS solapes,
          percentile_approx(unix_timestamp(hasta_anterior) - unix_timestamp(desde), 0.5) AS mediana_segundos
   FROM s WHERE desde < hasta_anterior GROUP BY fuente_desde;
   ```
5. **Corregir la regla del gemelo en `silver.periodos`:** si la línea aparece como cedida en `CHANGE_OWNER_LOG` y la baja del gemelo es posterior a la cesión, usar el STA. Implica regenerar periodos, transiciones, tenencia, la línea de tiempo y las inconsistencias.
6. **Validar con negocio** qué cambió en febrero de 2026 en las migraciones POS→PRE y qué son los estados `BYPASS` / `M_BYPASS`.