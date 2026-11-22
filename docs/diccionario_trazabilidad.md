# Diccionario de la trazabilidad de líneas

Qué significa cada tabla y columna que usa la línea de tiempo, **según lo que mostraron los datos** (caps. 3 a 8), con sus valores cerrados, su confiabilidad y sus relaciones.
Las cifras son agregados del export seudonimizado de noviembre de 2026.

## Convenciones

**Nombres en Databricks**
- `bronze.cm_pos__sub_mb` es `CM_POS.SUB_MB`: el esquema y la tabla de Oracle, separados por `__`.
- En bronze todo es texto.

**Funciones en silver**

| Función | Qué hace |
|---|---|
| `silver.f_isdn9(x)` | Deja el número en 9 dígitos (quita el 51 y lo que no sea dígito). Es la llave para cruzar por número. |
| `silver.f_ts(x)` | Convierte el texto de fecha de Oracle a timestamp. |
| `silver.f_estado_linea(tipo, status, act_status, con_baja)` | El estado de la línea en palabras. El mismo STATUS significa cosas distintas en PRE y en POS, y el 0 depende de si la línea tiene baja. |
| `silver.f_bloqueo(tipo, act_status)` | Los bloqueos de ACT_STATUS en palabras. |

**Tipo de línea:** `POS` es pospago (esquema CM_POS) y `PRE` es prepago (esquema CM_PRE). En las tablas de CM_POS que traen `DB_TYPE`: `0` = pospago y `1` = prepago.

## Mapa de relaciones

```
CM_POS.SUB_MB ── CONTRACT_ID ──> CM_POS.CONTRACT ── CUST_ID ──> CM_POS.CUSTOMER
CM_PRE.SUB_MB ── CUST_ID ──────────────────────────────────────> CM_POS.CUSTOMER

Por SUB_ID (+ DB_TYPE):
  SUB_MB.SUB_ID = CM_POS.ACTION_AUDIT.PK_ID (PK_TYPE 3)
                = CHANGE_OWNER_LOG.FROM_SUB_ID (línea cedida) / TO_SUB_ID (línea nueva)
                = TERMINATE_REQ / REQ_CHANGE_SIM / SUB_CHANGE_ISDN / AUTO_TERMINATE_SUB / AUTO_CANCEL_SUB .SUB_ID

Por número + fecha (no traen SUB_ID, o el que traen no sirve):
  CM_POS.TRANSFER_REQUEST · CM_PRE.TRANSFER_REQUEST · CM_PRE.REQUEST_TRANSFER_POSTPAID
  CM_PRE.STATUS · CM_PRE.TERMINATION_SUB · CM_PRE.SUB_SIM_TRANSFER
  MNP.PORTING_REQUEST.PORT_NUMBER ── STATE_CODE ──> MNP.STATE.CODE
        └─> se resuelven con silver.tenencia (qué línea tenía el número en esa fecha)
```

---

## CM_POS.SUB_MB · líneas pospago

Una fila por periodo de una línea pospago. **Llave:** `SUB_ID`. **Cliente:** `CONTRACT_ID` → `CM_POS.CONTRACT.CUST_ID`.

| Columna | Significado según los datos | Valores · ejemplo | Confiabilidad |
|---|---|---|---|
| SUB_ID | Identificador de la línea (del periodo) | texto numérico | Llave estable. Puede repetirse en CM_PRE: cruza siempre con el tipo. |
| ISDN | Número de la línea | se normaliza a 9 dígitos con `f_isdn9` | Bueno |
| STA_DATETIME | Alta del periodo. **Se hereda** al migrar desde prepago y en el cambio de titular. | 2019-03-01 10:00:00 | Heredado en el 28,22 % de las POS. En portabilidad es la fecha de ejecución. No sirve como inicio real. |
| MIG_DATETIME | Inicio real del periodo migrado PRE→POS | fecha | El 100 % está a 1 día o menos del trámite. |
| END_DATETIME | Baja del periodo | fecha o NULL | Si es NULL con STATUS 0, la línea se cedió en un cambio de titular (95,4 %). No significa que esté abierta. |
| STATUS | Estado (sin catálogo en AP_DOMAIN) | ver valores | La confianza de cada valor está en `gold.catalogo_valores`. |
| ACT_STATUS | Bloqueos, 3 dígitos: 1.º pedido por el cliente, 2.º por deuda, 3.º por límite de crédito | `000` normal · `010` · `020` | Catálogo oficial: AP_DOMAIN TYPE 306. Solo describe la línea si STATUS = 2. |

**STATUS (POS)**

| Valor | Significado |
|---|---|
| 2 | Activa |
| 3 | Cancelada |
| 1 | Registrada sin conectar (acción 00 sin 666) |
| 9 | Preactivada: suscriptor recibido (acción 666), aún sin conexión (00) |
| 0 con baja | Pasó a prepago (acción 167) |
| 0 sin baja | Cedida en un cambio de titular |
| 4 | Cerrada por cambio de titular, mecanismo antiguo (2021) |

**ACT_STATUS, cada dígito:** 0 = sin bloqueo · 1 = una vía (salientes) · 2 = dos vías.

## CM_PRE.SUB_MB · líneas prepago

Igual que la pospago, con estas diferencias:
- **Cliente:** `CUST_ID` → `CM_POS.CUSTOMER`.
- Los valores de STATUS 0, 4 y 9 **no** significan lo mismo que en pospago.

| Columna | Significado según los datos | Valores · ejemplo | Confiabilidad |
|---|---|---|---|
| STA_DATETIME | Alta del periodo; se hereda en la migración POS→PRE | fecha | En altas nuevas, la acción 00 calza a 15 min en el 96,1 % |
| MIG_DATETIME | Inicio real del periodo migrado POS→PRE | fecha | El 100 % está a 1 día o menos de `CM_PRE.TRANSFER_REQUEST` |
| END_DATETIME | Baja del periodo | fecha o NULL | Si es NULL con STATUS 0: cedida (99,4 % si no es kit) o kit borrado (ACT_STATUS con 3) |
| ACT_STATUS | Bloqueos, 2 dígitos: 1.º pedido por el cliente, 2.º por el sistema | `00` normal · `02` · `20` · `03` · `04` | AP_DOMAIN TYPE 02 + CM_PRE TYPE 100. 2.º dígito 3 = kit preactivado; 2.º dígito 4 = sin saldo. |

**STATUS (PRE)**

| Valor | Significado |
|---|---|
| 2 | Activa. Si ACT_STATUS tiene un 3, es un kit sin primer uso. |
| 3 | Cancelada |
| 1 | Registrada sin conectar |
| 0 | Eliminada: kit nunca usado, cerrada al pasar a pospago (con baja) o cedida (sin baja) |
| 4 | Cancelada en el cierre masivo de kits del 13/06/2019 |
| 9 | Sin significado documentado |

## CM_POS.CHANGE_OWNER_LOG · cambio de titular

Una fila por cambio. Cubre pospago y prepago (`DB_TYPE`). Hay datos desde el 10/11/2021.

| Columna | Significado | Confiabilidad |
|---|---|---|
| FROM_SUB_ID | Línea **cedida**. Queda STATUS 0 sin baja. | 95,4 % (POS) y 99,4 % (PRE) quedan sin baja. Solo 12 casos ya tenían baja antes del cambio (R3). |
| TO_SUB_ID | Línea **nueva** del nuevo titular. Hereda el STA de la cedida. | Siempre distinta de FROM_SUB_ID |
| FROM_CUST_ID / TO_CUST_ID | Cliente anterior y nuevo | Bueno |
| CREATE_TIME | Fecha del cambio. Es el inicio real de la línea nueva. | Bueno |
| DB_TYPE | 0 = pospago, 1 = prepago | Bueno |

## Trámites de migración PRE↔POS

| Tabla | Sentido | Llave útil | Nota |
|---|---|---|---|
| CM_POS.TRANSFER_REQUEST | PRE→POS | ISDN + REQUEST_DATETIME | `NEW_SUB_ID` **no es** el SUB_ID del pospago nuevo: solo lo es en el 18,4 % de los casos (0,7 % en 2026). No uses `NEW_SUB_ID` como llave. |
| CM_PRE.REQUEST_TRANSFER_POSTPAID | PRE→POS (cola de pedidos) | ISDN + CREATE_DATE | Trae el SUB_ID prepago, pero no el pospago. |
| CM_PRE.TRANSFER_REQUEST | POS→PRE | ISDN + fecha | — |

Las tres se unen en `silver.tramites_migracion`. Una migración "tiene trámite" si hay uno del mismo sentido para el número, a 3 días o menos.

## CM_POS.ACTION_AUDIT · acciones del CRM

- `PK_ID` = `SUB_ID` cuando `PK_TYPE` = 3.
- Acciones usadas: `00` = conexión o alta, `666` = suscriptor recibido (preactivación), `167` = cierre por paso a prepago.
- El hito de alta es la acción `00`, a 15 minutos como máximo del STA_DATETIME.

## CM_PRE.STATUS · log de estados prepago

- No trae SUB_ID. **Llave:** `ISDN`.
- Columnas: `NEW_STATUS` (estado nuevo), `OLD_STATUS` (anterior) y `DATE_CHANGE` (fecha del cambio).
- Es muy repetitivo: en la muestra de E6.4, 91.949 de 93.886 eventos repiten el estado. Para ver el estado en el tiempo usa `gold.estado_linea_pre_scd2` (AUTO CDC SCD2).

## MNP.PORTING_REQUEST · portabilidad y retorno de números

Una fila por solicitud. **No trae SUB_ID ni ISDN:** el número es `PORT_NUMBER`, y la línea se encuentra por número + fecha en `silver.tenencia`. Solo hay datos desde el 15/03/2026.

| Columna | Significado | Valores |
|---|---|---|
| PORT_NUMBER | Número portado | Calza con SUB_MB en el 93,2 % (entradas) y 99,9–100 % (resto) |
| TYPE | Tipo y sentido | `01` entrada · `05` salida · `02` retorno del número a Bitel · `06` Bitel devuelve el número a su operador original |
| RECEIVER_CODE / TRANSFEROR_CODE | Operador receptor / cedente | `24` = Bitel. En 01 y 02 Bitel recibe; en 05 y 06 Bitel cede. |
| STATE_CODE | Estado de la solicitud | Ver `MNP.STATE` y la tabla de abajo |
| CREATE_DATE | Fecha de la solicitud | Se usa como fecha del evento en las BYPASS |
| EXECUTION_DATE | Ejecución de la portabilidad | Solo la tienen 00A02 y 01A06. Mediana de 8 h desde la solicitud. |
| PORT_IN_ACTIVE_DATE | Activación en Bitel | Solo en el 61,1 % de las 00A02; nunca en las BYPASS |

**Estados que importan**

| STATE_CODE | Significado (MNP.STATE) | Uso |
|---|---|---|
| 00A02 | Process finished successfully | **Ejecutada** |
| 01A06 | Porting successfully completed | **Ejecutada** |
| BYPASS · M_BYPASS | No están en MNP.STATE; no tienen fecha de ejecución | **Entrada real**: en el 82–88 % nace una línea Bitel en ≤ 10 días. Su fecha es la de la solicitud. |
| 05A03 · 05D01 · 05R01 | Consulta previa (correcta, recibida del ABDCP, recibida del receptor) | No es portación |
| 01D01 · 01R02 · 01A05 · 01A03 | En proceso, programación o rechazo del cedente | No ejecutada |
| 01A07 | Preactived and created sale trans | Intermedia: queda fuera |
| 02R02 | refuse return | Retorno rechazado |
| 02D04 | Sent Number Return to Original Assignee | Tiene fecha, pero su estado es de rechazo: queda fuera |
| -1 · 0 | Canceled · New | No ejecutada |

`MNP.STATE` trae `CODE`, `NAME`, `DESCRIPTION` y `SM_CODE`. `SM_CODE` agrupa los estados: PROCESSING, ACCEPTED, REJECTED, FINISHED, ERROR, CANCELLED, GETSIM, NEW.

Las columnas de contacto y validación (`CONTACT_NAME`, `EMAIL_CONTACT`, `PHONE_CONTACT`, `FAX_CONTACT`, `ID_NO`, `IP_ADDRESS`, `OTP_ABDCP`) no se usan. Ver `politica_datos.md`.

## Bajas

| Tabla | Qué es realmente | Llave | Fechas | Nota |
|---|---|---|---|---|
| CM_POS.TERMINATE_REQ | Solicitud de baja (POS y PRE, por `DB_TYPE`) | SUB_ID | `CREATE_TIME` solicitud · `EXECUTE_DATE` ejecución | No tiene REQ_DATE. Datos desde 2020. Campos: `STATUS`, `REASON_ID`. |
| CM_PRE.TERMINATION_SUB | **Registro de la baja ya hecha** (no es una solicitud) | ISDN + fecha | `IMPORT_DATETIME` registro · `TERMINATE_TIME` ejecución | El 98,8 % cae justo después del fin de la línea (≤ 3 días). Campos: `STATE`, `LAST_STATE`, `TERMINATE_REASON`, `REASON`, `VALID`. |
| CM_POS.AUTO_TERMINATE_SUB | Baja automática | SUB_ID | — | Explica el 1,8 % de las bajas pospago sin solicitud (R1) |
| CM_POS.AUTO_CANCEL_SUB | Cancelación automática | SUB_ID | — | — |

La fecha de baja confiable es `END_DATETIME` (`silver.lineas.baja_ts`). Las tablas de arriba explican el motivo.

## SIM y número

| Tabla | Qué es realmente | Llave | Fecha | Nota |
|---|---|---|---|---|
| CM_POS.REQ_CHANGE_SIM | Cambio de SIM pospago | SUB_ID | `PROCESS_DATE`, si no `CREATE_TIME` | Datos desde 03/2023 |
| CM_PRE.SUB_SIM_TRANSFER | **SIM asignada a un número prepago** (de `CHANGE_DATETIME` a `END_DATETIME`) | ISDN + fecha | `CHANGE_DATETIME` | El 97,6 % es la SIM del alta, no un cambio: los cambios reales son unos 78 mil. En la línea de tiempo se marcan con `SIM del alta`. |
| CM_POS.SUB_CHANGE_ISDN | Cambio de número | SUB_ID | `PROCESS_DATE`, si no `CREATE_TIME` | `OLD_ISDN` → `NEW_ISDN`; `STATUS` 1 = éxito; `DB_TYPE` |

## AP_DOMAIN · catálogos

- ACT_STATUS pospago: `CM_POS.AP_DOMAIN` TYPE 306.
- ACT_STATUS prepago: `CM_POS.AP_DOMAIN` TYPE 02 y `CM_PRE.AP_DOMAIN` TYPE 100.
- **STATUS no tiene catálogo:** sus significados se dedujeron de los datos y están en `gold.catalogo_valores`.
- La vista `silver.v_dominio` expone los catálogos.

---

## Tablas construidas

| Tabla | Grano | Qué responde |
|---|---|---|
| `silver.lineas` | Una fila por línea (PRE y POS juntas) | Tipo, sub_id, isdn9, cliente, estado, fechas de alta, baja y migración |
| `silver.gemelos` | Par PRE/POS del mismo número con STA a < 60 s | Dónde el STA es heredado (28,22 % de las POS) |
| `silver.periodos` | Línea + inicio efectivo | `inicio_ts` y `fuente_inicio`: MIG_DATETIME si es coherente; si no, la baja del gemelo; si no, el STA |
| `silver.transiciones` | Cambio de línea dentro de un número | Sentido (`PRE→POS`…), días sin línea, si es el mismo cliente |
| `silver.tramites_migracion` | Trámite | Las 3 fuentes de trámite PRE↔POS con sentido, número y fecha |
| `silver.tenencia` | Periodo de una línea | **Quién tuvo el número, desde y hasta cuándo.** `desde` también considera el cambio de titular; `hasta` = baja, cesión o la siguiente línea. `fuente_desde` y `fuente_hasta` dicen de dónde salió cada fecha. |
| `silver.estado_actual_pre` | Número prepago | Último estado según `CM_PRE.STATUS` (MERGE incremental, cap. 5) |
| `gold.estado_linea_pre_scd2` | Número + vigencia | Historia del estado prepago (pipeline, AUTO CDC SCD2) |
| `gold.migraciones_por_mes` · `gold.bajas_por_mes` | Mes | Agregados del pipeline del cap. 6 |
| `gold.linea_tiempo` | Evento | ~230 M eventos de 12 tipos (14 combinaciones de fuente y evento) con fecha real, `sub_id`, fuente y detalle. En los eventos cruzados por número, el detalle dice cómo se halló la línea. |
| `gold.inconsistencias` | Caso | Reglas R1 a R5 (ver `hallazgos.md`) |
| `gold.catalogo_valores` | Valor de una columna | Significado y confianza de cada valor de STATUS / ACT_STATUS |
| `gold.diccionario_columnas` | Columna de bronze | Comentario, significado según los datos, confiabilidad y fuente recomendada |

**Eventos de `gold.linea_tiempo`:**
- ALTA, BAJA
- MIGRACION, RECICLAJE
- CAMBIO_TITULAR, CAMBIO_NUMERO, CAMBIO_SIM
- SOLICITUD_BAJA
- PORTABILIDAD_IN, PORTABILIDAD_OUT
- RETORNO_IN, RETORNO_OUT

## Qué fuente usar para cada pregunta

| Pregunta | Usa | No uses |
|---|---|---|
| ¿Desde cuándo tiene alguien el número? | `silver.tenencia.desde` | STA_DATETIME (se hereda) |
| ¿Quién era el titular en una fecha? | `silver.tenencia` + cliente (POS por CONTRACT) | La línea vigente de hoy |
| ¿En qué estado está la línea? | `silver.f_estado_linea(tipo, status, act_status, baja_ts IS NOT NULL)` | STATUS solo |
| ¿Se migró y cuándo? | `silver.transiciones` (mismo cliente, cambia el tipo) + trámite por número y fecha | `NEW_SUB_ID` de TRANSFER_REQUEST |
| ¿Se portó? | `gold.linea_tiempo` PORTABILIDAD_* (incluye BYPASS) | Solo los estados 00A02/01A06 |
| ¿Cambió de titular? | `CHANGE_OWNER_LOG` (FROM = cedida, TO = nueva) | STATUS 0 sin baja leído como "abierta" |
| ¿Cambió de SIM? | POS: `REQ_CHANGE_SIM`. PRE: `SUB_SIM_TRANSFER` sin las marcadas `SIM del alta`. | `SUB_SIM_TRANSFER` completa |
| ¿Cuándo se dio de baja? | `silver.lineas.baja_ts`. Motivo: TERMINATE_REQ, TERMINATION_SUB o AUTO_*. | TERMINATION_SUB como solicitud |
| ¿Cómo cambió el estado prepago? | `gold.estado_linea_pre_scd2` | `CM_PRE.STATUS` crudo (repite estados) |
| ¿El número fue reciclado? | Eventos RECICLAJE (otro cliente, línea anterior con baja, sin cambios de titular) | Cualquier cambio de cliente |
