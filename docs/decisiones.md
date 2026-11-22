# Decisiones de diseño

Registro de las decisiones del proyecto: qué se decidió, por qué y qué consecuencia tiene. Ordenado por capítulo.

## Datos y plataforma

**D1 · Trabajar con el export seudonimizado de BCCS, no con datos sintéticos**
- **Por qué:** las reglas (STA heredado, STATUS 0, cambio de titular) solo se pueden validar con la distribución real de los datos.
- **Consecuencia:** nada de datos entra al repo (`politica_datos.md`), y `00b_verificar_datos` corre antes de cualquier ejercicio.

**D2 · Databricks Free Edition**
- Solo hay cómputo serverless, una cuota diaria, un pipeline activo por tipo y salida a internet restringida.
- **Consecuencias:**
  - No hay grupos de cuenta ni external locations: los `GRANT` a grupos y la federación se practican como sintaxis o teoría.
  - Los jobs no se programan cada pocos minutos (el schedule queda en pausa).
  - El bundle se despliega desde la CLI local (D12).

**D3 · Cómputo (E1.4)**
- Notebooks serverless para desarrollar.
- SQL warehouse para dashboards y consultas de BI: arranca rápido y tiene caché de resultados.
- Job serverless para lo nocturno: paga solo lo que corre y no depende de un cluster encendido.
- El Query Profile es la herramienta para comparar cuántos archivos lee cada consulta.

## Delta Lake (cap. 2)

**D4 · Bronze guarda todo como texto** (`read_files` con `inferColumnTypes => false`)
- **Por qué:** los códigos tienen ceros a la izquierda que la inferencia de tipos destruye: ACT_STATUS `010`, acción `00`, `STATE_CODE` `00A02`, `TYPE` `01`. Las fechas de Oracle tampoco llegan en un formato uniforme.
- **Consecuencia:** los tipos se fijan en silver con funciones propias (`f_ts`, `f_isdn9`).

**D5 · `SHALLOW CLONE` para experimentar, `DEEP CLONE` para copias independientes**
- Shallow solo copia metadatos: es instantáneo y barato. Sirve para probar un `UPDATE` o `DELETE` sin tocar la original, pero depende de los archivos de la tabla original (un `VACUUM` allá lo rompe).
- Deep copia los datos: sirve para respaldos, migraciones o compartir una copia que no dependa del original.

**D6 · Liquid clustering por `isdn9`**
- **Por qué:** casi todas las consultas y cruces son por número (línea de tiempo, tenencia, inconsistencias).
- **Consecuencia:** `CLUSTER BY (isdn9)` en `silver.tenencia`, `gold.linea_tiempo` y las tablas grandes. A diferencia de particionar, la columna se puede cambiar sin reescribir la tabla.

## Transformaciones (caps. 3 y 4)

**D7 · Inicio efectivo de un periodo (E4.4)**
- **Regla:** `MIG_DATETIME` si es coherente (entre el alta y la baja); si no, la **baja del gemelo** (línea del otro tipo, mismo número, STA a < 60 s); si no, el STA.
- **Por qué:** el STA se hereda en el 28,22 % de las altas pospago. El MIG_DATETIME está a 1 día o menos del trámite en el 100 % de los casos.
- **Limitación descubierta en el cap. 8:** la regla del gemelo da fechas imposibles en 68 mil periodos de líneas que luego se cedieron (inicio posterior a la cesión).
- **Corrección propuesta:** si la línea aparece como cedida en `CHANGE_OWNER_LOG` y la baja del gemelo es posterior a la cesión, usar el STA.

**D8 · El estado de una línea se lee con una función, no con STATUS**
- `silver.f_estado_linea(tipo, status, act_status, con_baja)`.
- **Por qué:** STATUS 0, 4 y 9 significan cosas distintas en PRE y POS. Además, el STATUS 0 es ambiguo sin mirar la baja: sin baja es línea cedida (95,4 % POS, 99,4 % PRE); con baja, paso al otro tipo.
- Los significados viven en `gold.catalogo_valores`, porque STATUS no tiene catálogo en AP_DOMAIN.

**D9 · Las migraciones se cruzan por número + fecha, no por `NEW_SUB_ID`**
- **Por qué:** en `CM_POS.TRANSFER_REQUEST` el `NEW_SUB_ID` solo es el SUB_ID pospago en el 18,4 % de los casos (0,7 % en 2026).
- **Regla:** hay trámite si existe uno del mismo sentido para el número a ±3 días.

## Incremental (cap. 5)

**D10 · El estado prepago se mantiene con MERGE por número**
- **Por qué:** `CM_PRE.STATUS` no trae SUB_ID: la llave es `ISDN` y el estado está en `NEW_STATUS`.
- **Consecuencia:** `silver.estado_actual_pre` se actualiza con `MERGE` sobre `isdn9`.

## Pipelines, jobs y bundles (cap. 6)

**D11 · El pipeline es un archivo `.sql`, no un notebook**
- **Por qué:** el editor nuevo de pipelines trabaja con archivos dentro de la carpeta raíz. Un notebook en la misma ruta no se reconoce como el archivo `.sql` que pide el bundle.
- **Consecuencia:** `notebooks/06b_pipeline_trazabilidad.sql` es SQL puro, sin encabezado de notebook, y el bundle apunta a ese único archivo.
- **Contenido:**
  - Streaming tables para bronze y silver.
  - `AUTO CDC ... STORED AS SCD TYPE 2` con `TRACK HISTORY` para el estado prepago.
  - Materialized views para los agregados.
  - Expectations con `warn`, `DROP ROW` y `FAIL UPDATE`, según la gravedad.

**D12 · Declarative Automation Bundle desplegado desde la CLI local**
- `databricks.yml` está en la raíz y define el pipeline `trazabilidad_bitel` y el job `job_trazabilidad`. El target es `dev` (`mode: development`, con prefijo `[dev brando_rojas]`).
- **Por qué la CLI:** en Free Edition el panel de bundles del workspace se queda cargando, porque no puede descargar Terraform. Se usa `databricks bundle deploy -t dev -p dbc-942fe594-cbd9`.
- **Consecuencias aprendidas:**
  - Un job gestionado por el bundle no se puede reparar ni editar desde la UI: se relanza con `databricks bundle run ... --no-wait`.
  - Las tablas de un pipeline borrado siguen asociadas a él durante el periodo de restauración. Para que otro pipeline las gestione hay que borrarlas (`DROP`) o reasignar `pipelines.pipelineId`.

**D13 · Job de tres tareas: llegada → pipeline → control**
- **llegada (06c):** copia `dias` días nuevos de `CM_PRE.ACTION_AUDIT` a la carpeta de llegadas, simulando la llegada diaria. Tiene 1 reintento.
- **pipeline:** refresca `trazabilidad_bitel` y procesa solo lo nuevo.
- **control (06d):** falla si no llegó nada o si las filas descartadas entre bronze y silver superan `umbral_descartes` (5 %). No tiene reintentos, porque reintentar un control no lo arregla.
- **`performance_target: STANDARD`:** es más barato, pero suma de 4 a 6 minutos de arranque por tarea. Para iterar rápido conviene `PERFORMANCE_OPTIMIZED`.
- Avisos por correo si falla. El schedule es diario a las 6:00 (Lima), en pausa.

## Gobierno (cap. 7)

**D14 · Etiquetas para clasificar y comentarios para explicar**
- Las etiquetas (`pii`, `clasificacion`, `dominio`) se pueden consultar en `information_schema` y sirven para políticas.
- Los comentarios explican el significado y alimentan `gold.diccionario_columnas`.

**D15 · Máscara y filtro probados y retirados.** Mientras están activos, bloquean el time travel, los clones y Delta Sharing de la tabla. En producción se pondrían por grupo (`politica_datos.md`).

**D16 · Delta Sharing para OSIPTEL (E7.6, pregunta 1)**
- **Técnicamente sí:** el *open sharing* entrega datos a quien no usa Databricks, mediante un archivo de credenciales y un cliente estándar.
- **Qué compartiría:**
  - Una **vista** con altas y bajas del periodo: fecha, tipo de línea, tipo de evento y, solo si el requerimiento lo exige, el número.
  - Nunca `cust_id`, documento, contactos, columnas de validación ni las tablas silver completas.
- **Condición:** no se puede compartir una tabla con máscara o filtro activos, otra razón para compartir una vista.
- **En la práctica:** OSIPTEL recibe la información por sus propios canales y formatos, así que Sharing serviría para un intercambio recurrente acordado, no para reemplazar el reporte regulatorio.

**D17 · Lakehouse Federation para Oracle REPORT (E7.6, pregunta 2)**
- **Qué permite:** consultar Oracle sin copiar los datos, con una *connection* y un *foreign catalog*.
- **Limitaciones frente al export a Parquet:**
  - Cada consulta carga la base de producción.
  - El rendimiento depende de Oracle y de la red, y no todo se empuja a Oracle.
  - No hay time travel ni historia propia.
  - Los resultados cambian mientras Oracle cambia, así que no son reproducibles.
  - En Free Edition no se puede probar, porque no hay salida hacia la base de Bitel.
- **El export conviene** para analizar historia y reproducir resultados. **La federación conviene** para consultas puntuales a datos vivos.

## Cierre (cap. 8)

**D18 · `silver.tenencia`: el historial de quién tuvo cada número**
- **desde** = inicio efectivo (D7), o la fecha del cambio de titular si la línea nació de uno.
- **hasta** = la baja; si no tiene, la cesión; si no, el inicio de la siguiente línea; si no, NULL (vigente).
- **Por qué:** `MNP.PORTING_REQUEST`, `CM_PRE.TERMINATION_SUB` y `CM_PRE.SUB_SIM_TRANSFER` solo traen número + fecha. La regla de cruce se escribe una vez y todos la reutilizan.

**D19 · Regla de cruce por número + fecha**
- Gana el periodo que contiene la fecha. Si no hay ninguno, gana el más cercano a ≤ 3 días; si empatan, el más reciente.
- Las tablas de CM_PRE solo cruzan con líneas prepago.
- **Ningún evento se pierde:** si no hay línea, el evento queda con `sub_id` NULL, y el detalle dice cómo se encontró (`linea=dentro del periodo`, `linea=a N h del periodo`, `linea=ninguna a 3 dias`).

**D20 · Qué cuenta como portabilidad**
- **Ejecutadas:** `00A02` y `01A06` (con fecha de ejecución), más `BYPASS` y `M_BYPASS`. Estas últimas usan la fecha de la solicitud, porque en el 82–88 % nace una línea Bitel en ≤ 10 días.
- Se agregan los **retornos de número** (TYPE 02 y 06).
- **Quedan fuera:**
  - `02D04`: tiene fecha, pero su estado es de rechazo.
  - `01A07`: es un estado intermedio.

**D21 · Las reglas de inconsistencia, ajustadas a lo que los datos permiten**
- **R1 solo en pospago:** en prepago casi todas las bajas las hace el sistema (vencimiento, kits) y no llevan solicitud.
- **R1 mide cobertura de logs:** para que mida errores hay que restringirla a la ventana común de las fuentes (desde 03/2026).
- **R3 redefinida:** una línea cedida normalmente no tiene baja. Lo raro es que ya la tuviera antes del cambio.
- **R5 cruza trámites por número**, no por `NEW_SUB_ID`.

**D22 · Qué se publica**
- `docs/hallazgos.md` solo tiene agregados.
- Las tablas de requerimientos judiciales se consultan solo con conteos.
- Ninguna cifra del repo permite llegar a una persona.