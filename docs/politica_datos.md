# Política de datos del proyecto

Cómo se tratan los datos personales en este lakehouse de práctica, y qué haría distinto en un workspace corporativo.

## 1. Qué columnas son personales

| Tipo | Columnas | Tratamiento |
|---|---|---|
| Identifican directamente a una persona o una línea | `ISDN`, `PORT_NUMBER`, `OLD_ISDN`, `NEW_ISDN`, `ID_NO`, `CONTACT_NAME`, `EMAIL_CONTACT`, `PHONE_CONTACT`, `FAX_CONTACT`, `IP_ADDRESS` | Seudonimizadas en origen. Solo `ISDN` y `PORT_NUMBER` se usan, normalizados a `isdn9`. |
| Identifican de forma indirecta | `CUST_ID`, `SUB_ID`, `CONTRACT_ID`, `SERIAL`, `IMSI`, `SIM_SERIAL` | Seudonimizadas. Sirven para cruzar, nunca para mostrar. |
| Validación de un trámite | `OTP_ABDCP` (MNP.PORTING_REQUEST) | Ningún ejercicio la usa: se elimina (ver sección 5). |
| Sensibles por contexto | Tablas de requerimientos judiciales (LSC), con DNI de personas investigadas | Solo se consultan con agregados (conteos). Nunca filas. |

El dato más sensible no es una columna, sino la **combinación**: número + titular + fechas reconstruye la historia de una persona. Por eso la línea de tiempo se trata entera como confidencial.

## 2. Cómo se seudonimizó antes de subir

- Los identificadores se reemplazaron **en origen, antes de salir de Oracle**. Se conservan los nombres de tablas y columnas, para que las reglas de negocio sigan siendo válidas.
- `00b_verificar_datos` revisa una muestra de cada tabla (20 mil filas, umbral del 30 %) con patrones de celular, DNI, correo y nombre completo. Un hallazgo en una columna de número o documento detiene el trabajo. Un hallazgo en un ID interno de 8 dígitos se documenta como falso positivo.

## 3. Qué se aplicó en Databricks (cap. 7)

**Etiquetas en `silver.lineas`**
- `isdn9`: `pii = telefono`.
- `cust_id`: `pii = cliente`.
- La tabla: `clasificacion = confidencial` y `dominio = lineas_moviles`.

**Máscara de columna** (`gobierno.mascara_numero`): el propietario ve el número completo; los demás, solo los últimos 4 dígitos.

**Filtro de filas** (`gobierno.solo_prepago`): el propietario ve todo; los demás, solo prepago.

La máscara y el filtro se probaron y **se retiraron**, porque mientras están activos bloquean el time travel, los clones y Delta Sharing de la tabla. Las funciones quedan guardadas en el esquema `gobierno`.

**Permisos mínimos** para leer una tabla:
- `USE CATALOG` sobre el catálogo.
- `USE SCHEMA` sobre el esquema.
- `SELECT` sobre la tabla.

Free Edition no tiene grupos de cuenta, así que los `GRANT` a grupos se practicaron como sintaxis, y el ciclo completo se probó con `account users` y luego se revocó.

**Linaje:** `system.access.table_lineage` muestra de qué tablas sale cada tabla gold.

## 4. Qué haría en un workspace corporativo

1. **Máscaras y filtros por grupo, no por correo:** `is_account_group_member('pmo_trazabilidad')` ve el número completo; el resto ve los últimos 4 dígitos.
2. **Filtro de filas por finalidad.** Por ejemplo, Legal ve solo los números de un requerimiento vigente, con una tabla de autorizaciones.
3. **Gold para consumo general solo con agregados o vistas sin identificadores.** Las tablas con `isdn9` quedan en silver, con acceso nominal.
4. **Auditoría** con `system.access.audit` (quién leyó qué) y revisión periódica de los permisos.
5. **Retención:** borrar del lakehouse los logs que superen el plazo que exige la regulación, con `DELETE` + `VACUUM`. Hay que tener en cuenta que el time travel conserva lo borrado hasta el `VACUUM`.

## 5. Por qué los datos nunca entran al repo

- **El repo es código y documentación.**
  - Los datos personales, aun seudonimizados, siguen siendo datos personales mientras sea posible reidentificarlos (Ley 29733 y su reglamento, D.S. 016-2024-JUS).
  - El contenido de las comunicaciones y su registro están protegidos por el secreto de las telecomunicaciones.
- **`.gitignore`** bloquea `*.parquet`, `*.csv`, `*.xlsx` y `EXPORT_*/`.
- **`docs/hallazgos.md`** solo tiene agregados: ninguna fila, número ni cliente.
- **Las capturas de resultados** para el repo o una entrevista se toman de consultas agregadas.

**Pendiente: quitar `OTP_ABDCP` de bronze.** Ningún ejercicio la usa (minimización):

```sql
CREATE OR REPLACE TABLE bronze.mnp__porting_request AS
SELECT * EXCEPT (OTP_ABDCP) FROM bronze.mnp__porting_request;
```

Después, en el próximo export, no incluirla. Las versiones anteriores de la tabla la conservan hasta que corra un `VACUUM` pasado el periodo de retención.